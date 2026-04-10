"""
Load trained models and make predictions on new data.

This module handles:
- Loading trained models from disk
- Preparing features for new data
- Making predictions
- Returning risk levels
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
import json
import joblib
from typing import Dict, List, Optional, Tuple

# Suppress noisy pandas warnings
pd.options.mode.chained_assignment = None
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=UserWarning)
warnings.filterwarnings('ignore', message='.*SettingWithCopyWarning.*')

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from ml_training.config import (
    HAZARD_MODEL_VERSIONS,
    MODELS_DIR,
    REGION_DESCRIPTOR_ENCODINGS_FILENAME,
    REQUIRED_FEATURES,
)
from ml_training.data_preparation.load_training_data import (
    load_era5_data,
    load_modis_data,
    load_viirs_data,
    load_terrain_data,
    load_region_descriptors,
    load_river_discharge_data,
    merge_datasets,
    handle_missing_data,
)
from ml_training.data_preparation.descriptor_encoding import (
    apply_descriptor_string_encodings,
    load_descriptor_encodings,
)
from ml_training.data_preparation.feature_engineering import engineer_features
from ml_training.models.train_models import (
    get_feature_columns,
    remove_correlated_features,
)


class ModelPredictor:
    """Predictor for disaster risk models."""
    
    def __init__(self, disaster_type: str):
        """Initialize predictor for a disaster type."""
        self.disaster_type = disaster_type
        self.model = None
        self.metadata = None
        self.feature_columns = None
        self.descriptor_string_encodings = {}
        self.class_thresholds = {}
        self._load_model()
    
    def _load_model(self) -> None:
        """Load model and metadata from disk."""
        version = HAZARD_MODEL_VERSIONS.get(self.disaster_type, "1.3")
        model_path = MODELS_DIR / f"{self.disaster_type}_model_v{version}.pkl"
        metadata_path = MODELS_DIR / f"{self.disaster_type}_model_v{version}_metadata.json"
        
        if not model_path.exists():
            raise FileNotFoundError(f"Model not found: {model_path}")
        
        import warnings
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="Trying to unpickle estimator .* from version .* when using version .*",
            )
            self.model = joblib.load(model_path)
        
        with open(metadata_path, 'r') as f:
            self.metadata = json.load(f)
        
        self.feature_columns = self.metadata['features']
        self.descriptor_string_encodings = self.metadata.get('descriptor_string_encodings') or {}
        self.class_thresholds = self.metadata.get("class_thresholds") or {}
        schema = self.metadata.get("label_schema") or {}
        self.landslide_ui_l3_threshold = float(
            schema.get("inference_high_confidence_level_3_threshold", 0.70)
        )
        self.n_classes = int(self.metadata.get("n_classes") or len(self.model.classes_))
        if not self.descriptor_string_encodings:
            version = HAZARD_MODEL_VERSIONS.get(self.disaster_type, "1.3")
            enc_path = MODELS_DIR / REGION_DESCRIPTOR_ENCODINGS_FILENAME.format(
                version=version
            )
            if enc_path.exists():
                self.descriptor_string_encodings = load_descriptor_encodings(enc_path)
    
    def prepare_features(
        self,
        regions: List[str],
        start_date: str,
        end_date: str,
        dataset_id: Optional[str] = None,
        preloaded_data: Optional[pd.DataFrame] = None,
        preloaded_static: Optional[Dict[str, pd.DataFrame]] = None
    ) -> pd.DataFrame:
        """Prepare features for prediction.
        
        Args:
            regions: List of region names
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            dataset_id: Dataset to load from ('climatology' or 'daily_ingestion'), 
                       or None to auto-detect based on date range
            preloaded_data: Optional DataFrame containing pre-fetched ERA5, MODIS, VIIRS, etc.
            preloaded_static: Optional dict with 'terrain' and 'desc' DataFrames
        """
        if preloaded_data is not None and not preloaded_data.empty:
            # Filter preloaded data for requested regions and dates
            merged = preloaded_data[
                (preloaded_data['region'].isin(regions)) & 
                (preloaded_data['date'] >= pd.to_datetime(start_date)) & 
                (preloaded_data['date'] <= pd.to_datetime(end_date))
            ].copy()
            
            # Load static data (terrain and descriptors)
            if preloaded_static:
                terrain = preloaded_static.get('terrain', pd.DataFrame())
                desc = preloaded_static.get('desc', pd.DataFrame())
            else:
                terrain = load_terrain_data(regions=regions)
                desc = load_region_descriptors(regions=regions)
            
            if not terrain.empty:
                terrain_cols = [col for col in terrain.columns if col != 'region']
                # Filter terrain for these regions
                t_chunk = terrain[terrain['region'].isin(regions)]
                if not t_chunk.empty:
                    merged = merged.merge(t_chunk[['region'] + terrain_cols], on='region', how='left')
            
            if not desc.empty:
                desc_cols = [col for col in desc.columns if col not in ['region', 'elevation_mean_m', 'slope_mean_deg']]
                # Filter desc for these regions
                d_chunk = desc[desc['region'].isin(regions)]
                if not d_chunk.empty:
                    merged = merged.merge(d_chunk[['region'] + desc_cols], on='region', how='left')
        else:
            era5 = load_era5_data(regions=regions, start_date=start_date, end_date=end_date, dataset_id=dataset_id)
            modis = load_modis_data(regions=regions, start_date=start_date, end_date=end_date, dataset_id=dataset_id)
            viirs = load_viirs_data(regions=regions, start_date=start_date, end_date=end_date, dataset_id=dataset_id)
            terrain = load_terrain_data(regions=regions)
            desc = load_region_descriptors(regions=regions)
            
            merged = merge_datasets(era5, modis, viirs, terrain, desc, start_date, end_date)
            if self.disaster_type in ("flood", "landslide"):
                discharge_df = load_river_discharge_data(
                    regions=regions, start_date=start_date, end_date=end_date
                )
                if not discharge_df.empty:
                    merged["date"] = pd.to_datetime(merged["date"]).dt.normalize()
                    discharge_df = discharge_df.drop_duplicates(
                        subset=["date", "region"], keep="last"
                    )
                    merged = merged.merge(
                        discharge_df[["date", "region", "river_discharge"]],
                        on=["date", "region"],
                        how="left",
                    )
                    
        if merged.empty:
            return pd.DataFrame()

        merged = apply_descriptor_string_encodings(merged, self.descriptor_string_encodings)
        clean = handle_missing_data(merged, REQUIRED_FEATURES)
        
        # Free memory from intermediate dataframes
        del merged
        import gc
        gc.collect()
        
        features, _ = engineer_features(clean, compute_climatology_from_data=False)
        
        return features
    
    def predict(
        self,
        features: pd.DataFrame,
        return_proba: bool = False
    ) -> pd.Series:
        """Predict risk levels for features.
        
        Args:
            features: DataFrame with engineered features
            return_proba: If True, return probability predictions
            
        Returns:
            Series of risk levels (0-3) or DataFrame of probabilities
        """
        feature_cols = [c for c in self.feature_columns if c in features.columns]
        X = features[feature_cols].copy()
        
        missing_cols = set(self.feature_columns) - set(feature_cols)
        if missing_cols:
            for col in missing_cols:
                X[col] = 0
        
        X = X[self.feature_columns]
        X = X.select_dtypes(include=[np.number])
        
        if return_proba:
            proba = self.model.predict_proba(X)
            proba_df = pd.DataFrame(
                proba,
                index=features.index,
                columns=[f'prob_level_{i}' for i in range(proba.shape[1])]
            )
            return proba_df
        else:
            if self.class_thresholds:
                proba = self.model.predict_proba(X)
                col_idx = {int(c): i for i, c in enumerate(self.model.classes_)}
                t2 = float(self.class_thresholds.get("level_2", 0.5))
                t3 = float(self.class_thresholds.get("level_3", 0.5))
                t3_landslide_ui = float(self.landslide_ui_l3_threshold)
                preds = []
                n_cls = len(col_idx)
                i2 = col_idx.get(2)
                i3 = col_idx.get(3)
                for row in proba:
                    if self.disaster_type == "landslide" and n_cls == 3 and i2 is not None:
                        if row[i2] >= t3_landslide_ui:
                            preds.append(3)
                        elif row[i2] >= t2:
                            preds.append(2)
                        else:
                            preds.append(int(np.argmax(row)))
                        continue
                    if i3 is not None and row[i3] >= t3:
                        preds.append(3)
                    elif i2 is not None and row[i2] >= t2:
                        preds.append(2)
                    else:
                        preds.append(int(np.argmax(row)))
                predictions = np.array(preds, dtype=int)
            else:
                predictions = self.model.predict(X)
            return pd.Series(predictions, index=features.index, name='risk_level')
    
    def predict_for_regions(
        self,
        regions: List[str],
        start_date: str,
        end_date: str,
        return_proba: bool = False
    ) -> pd.DataFrame:
        """Predict risk levels for regions and date range.
        
        Returns:
            DataFrame with predictions (and optionally probabilities)
        """
        features = self.prepare_features(regions, start_date, end_date)
        predictions = self.predict(features, return_proba=return_proba)
        
        result = features[['date', 'region']].copy()
        
        if return_proba:
            result = pd.concat([result, predictions], axis=1)
        else:
            result['risk_level'] = predictions
        
        return result


def load_predictor(disaster_type: str) -> ModelPredictor:
    """Load a predictor for a disaster type."""
    return ModelPredictor(disaster_type)


def predict_all_disasters(
    regions: List[str],
    start_date: str,
    end_date: str,
    disaster_types: Optional[List[str]] = None
) -> Dict[str, pd.DataFrame]:
    """Predict risk levels for all disaster types.
    
    Returns:
        Dictionary mapping disaster_type to predictions DataFrame
    """
    if disaster_types is None:
        disaster_types = ['fire', 'drought', 'flood', 'landslide']
    
    results = {}
    
    for disaster_type in disaster_types:
        try:
            predictor = load_predictor(disaster_type)
            predictions = predictor.predict_for_regions(
                regions, start_date, end_date
            )
            results[disaster_type] = predictions
        except FileNotFoundError:
            print(f"Warning: Model not found for {disaster_type}, skipping...")
        except Exception as e:
            print(f"Error predicting {disaster_type}: {e}")
            raise
    
    return results


if __name__ == "__main__":
    from ml_training.data_preparation.load_training_data import load_all_regions
    
    regions = load_all_regions()[:5]
    start_date = "2024-01-01"
    end_date = "2024-01-31"
    
    print("Testing model predictor...")
    results = predict_all_disasters(regions, start_date, end_date, ['fire'])
    
    for disaster_type, df in results.items():
        print(f"\n{disaster_type} predictions:")
        print(df.head())
