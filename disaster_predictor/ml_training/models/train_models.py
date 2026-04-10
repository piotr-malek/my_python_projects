"""
Train Random Forest models for disaster detection.

This module handles:
- Loading training data for each disaster type
- Creating labels
- Chronological train/test split
- Training Random Forest models
- Saving models with metadata
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
import json
import joblib
import time
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.impute import SimpleImputer
from imblearn.over_sampling import SMOTE, SMOTENC
from imblearn.pipeline import Pipeline as ImbPipeline

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from ml_training.config import (
    HAZARD_MODEL_VERSIONS,
    MODELS_DIR,
    REGION_DESCRIPTOR_ENCODINGS_FILENAME,
    REGION_DESCRIPTOR_STRING_COLUMNS,
    RF_N_ESTIMATORS,
    RF_MAX_DEPTH,
    RF_MIN_SAMPLES_SPLIT,
    RF_MIN_SAMPLES_LEAF,
    RF_CLASS_WEIGHT,
    RF_RANDOM_STATE,
    TRAIN_TEST_SPLIT,
    CORRELATION_THRESHOLD,
    FIRE_TRAIN_START,
    FIRE_TRAIN_END,
    DROUGHT_TRAIN_START,
    DROUGHT_TRAIN_END,
    FLOOD_TRAIN_START,
    FLOOD_TRAIN_END,
    LANDSLIDE_TRAIN_START_ORIGINAL,
    LANDSLIDE_TRAIN_END_ORIGINAL,
    REQUIRED_FEATURES,
)
from ml_training.data_preparation.load_training_data import (
    load_all_regions,
    load_era5_data,
    load_modis_data,
    load_viirs_data,
    load_terrain_data,
    load_region_descriptors,
    load_glc_events,
    load_global_flood_db,
    load_worldfloods_events,
    load_river_discharge_data,
    merge_datasets,
    handle_missing_data,
)
from ml_training.data_preparation.feature_engineering import engineer_features
from ml_training.data_preparation.create_labels import (
    create_fire_labels,
    create_drought_labels,
    create_flood_labels,
    create_landslide_labels,
)
from ml_training.data_preparation.climatology_utils import load_climatology_from_bq
from ml_training.data_preparation.descriptor_encoding import (
    apply_descriptor_string_encodings,
    build_descriptor_encodings,
    save_descriptor_encodings,
)


def _progress(msg: str, t0: Optional[float] = None) -> None:
    """Wall-clock + optional elapsed since t0 (seconds); always flush for live terminals."""
    stamp = time.strftime("%H:%M:%S")
    extra = f"  (+{time.perf_counter() - t0:.1f}s)" if t0 is not None else ""
    print(f"[{stamp}] {msg}{extra}", flush=True)


def get_feature_columns(df: pd.DataFrame, disaster_type: str) -> List[str]:
    """Get feature columns for a disaster type, excluding metadata."""
    exclude = ['date', 'region', 'month', 'day_of_year'] + list(REGION_DESCRIPTOR_STRING_COLUMNS)
    
    feature_cols = [c for c in df.columns if c not in exclude]
    
    return feature_cols


def remove_correlated_features(
    X: pd.DataFrame,
    threshold: float = CORRELATION_THRESHOLD
) -> Tuple[pd.DataFrame, List[str]]:
    """Remove highly correlated features.
    
    Returns:
        X_filtered: DataFrame with correlated features removed
        removed_features: List of feature names that were removed
    """
    corr_matrix = X.corr().abs()
    upper_triangle = corr_matrix.where(
        np.triu(np.ones(corr_matrix.shape), k=1).astype(bool)
    )
    
    to_remove = [
        column for column in upper_triangle.columns
        if any(upper_triangle[column] > threshold)
    ]
    
    X_filtered = X.drop(columns=to_remove)
    
    return X_filtered, to_remove


def prepare_training_data(
    disaster_type: str,
    regions: Optional[List[str]] = None,
    descriptor_encodings: Optional[Dict] = None,
) -> Tuple[pd.DataFrame, pd.Series, pd.DataFrame, pd.Series, pd.Series, pd.Series]:
    """Load and prepare training data for a disaster type.
    
    Returns:
        X_train, y_train, X_test, y_test, sample_weight_train, sample_weight_test
    """
    if regions is None:
        regions = load_all_regions()
    
    date_ranges = {
        'fire': (FIRE_TRAIN_START, FIRE_TRAIN_END),
        'drought': (DROUGHT_TRAIN_START, DROUGHT_TRAIN_END),
        'flood': (FLOOD_TRAIN_START, FLOOD_TRAIN_END),
        'landslide': (LANDSLIDE_TRAIN_START_ORIGINAL, LANDSLIDE_TRAIN_END_ORIGINAL),
    }
    
    start_date, end_date = date_ranges[disaster_type]
    
    phase0 = time.perf_counter()
    _progress(f"prepare_training_data({disaster_type}): {start_date} → {end_date}, {len(regions)} regions")
    _progress("  loading ERA5 (BigQuery)...", phase0)
    era5 = load_era5_data(regions=regions, start_date=start_date, end_date=end_date)
    _progress(f"  ERA5 rows: {len(era5):,}", phase0)
    _progress("  loading MODIS/Landsat...", phase0)
    modis = load_modis_data(regions=regions, start_date=start_date, end_date=end_date)
    _progress(f"  MODIS/Landsat rows: {len(modis):,}", phase0)
    _progress("  loading VIIRS...", phase0)
    viirs = load_viirs_data(regions=regions, start_date=start_date, end_date=end_date)
    _progress(f"  VIIRS rows: {len(viirs):,}", phase0)
    _progress("  loading terrain_static...", phase0)
    terrain = load_terrain_data(regions=regions)
    _progress("  loading region descriptors...", phase0)
    desc = load_region_descriptors(regions=regions)
    
    _progress("  merging datasets...", phase0)
    merged = merge_datasets(era5, modis, viirs, terrain, desc, start_date, end_date)
    if disaster_type in ("flood", "landslide"):
        _progress("  loading river discharge (GloFAS + daily)...", phase0)
        discharge_df = load_river_discharge_data(regions=regions, start_date=start_date, end_date=end_date)
        if not discharge_df.empty:
            merged["date"] = pd.to_datetime(merged["date"]).dt.normalize()
            discharge_df = discharge_df.drop_duplicates(subset=["date", "region"], keep="last")
            merged = merged.merge(
                discharge_df[["date", "region", "river_discharge"]],
                on=["date", "region"],
                how="left",
            )
        _progress(f"  discharge merge done (rows loaded: {len(discharge_df):,})", phase0)
    enc = descriptor_encodings
    if enc is None:
        enc = build_descriptor_encodings(load_region_descriptors())
    merged = apply_descriptor_string_encodings(merged, enc)
    _progress("  handle_missing_data...", phase0)
    clean = handle_missing_data(merged, REQUIRED_FEATURES)
    _progress("  engineer_features (BQ climatology + rolling windows)...", phase0)
    features, _ = engineer_features(clean, compute_climatology_from_data=False)
    _progress(f"  features shape: {features.shape}", phase0)
    
    _progress(f"  creating labels ({disaster_type})...", phase0)
    if disaster_type == 'fire':
        labels, sample_weights = create_fire_labels(features)
    elif disaster_type == 'drought':
        labels, sample_weights = create_drought_labels(features)
    elif disaster_type == 'flood':
        _progress("    loading GFD + WorldFloods events...", phase0)
        gfd_events = load_global_flood_db(regions=regions)
        worldfloods_events = load_worldfloods_events(regions=regions)
        labels, sample_weights = create_flood_labels(
            features,
            climatology=None,
            gfd_events=gfd_events,
            worldfloods_events=worldfloods_events,
            return_confidence=True,
        )
    elif disaster_type == 'landslide':
        _progress("    loading GLC + climatology for proxies...", phase0)
        glc_events = load_glc_events(regions=regions)
        climatology = load_climatology_from_bq()
        labels, sample_weights = create_landslide_labels(features, glc_events, climatology)
    else:
        raise ValueError(f"Unknown disaster type: {disaster_type}")
    
    feature_cols = get_feature_columns(features, disaster_type)
    X = features[feature_cols].copy()
    y = labels
    
    valid_mask = y.notna()
    X = X[valid_mask]
    y = y[valid_mask]
    sample_weights = sample_weights[valid_mask]
    
    print(f"Total samples: {len(X):,}")
    print(f"Label distribution:")
    for level in sorted(int(x) for x in y.dropna().unique()):
        count = (y == level).sum()
        print(f"  Level {level}: {count:,} ({count/len(y)*100:.1f}%)")
    
    X = X.select_dtypes(include=[np.number])
    
    _progress("  remove_correlated_features...", phase0)
    X_filtered, removed = remove_correlated_features(X, CORRELATION_THRESHOLD)
    if removed:
        print(f"Removed {len(removed)} highly correlated features: {removed[:5]}...")
    
    features_sorted = features.sort_values(['region', 'date']).reset_index(drop=True)
    X_filtered = X_filtered.reindex(features_sorted.index)[valid_mask].reset_index(drop=True)
    y = y[valid_mask].reset_index(drop=True)
    
    split_idx = int(len(X_filtered) * TRAIN_TEST_SPLIT)
    X_train = X_filtered.iloc[:split_idx].reset_index(drop=True)
    X_test = X_filtered.iloc[split_idx:].reset_index(drop=True)
    y_train = y.iloc[:split_idx].reset_index(drop=True)
    y_test = y.iloc[split_idx:].reset_index(drop=True)
    w_train = sample_weights.iloc[:split_idx].reset_index(drop=True)
    w_test = sample_weights.iloc[split_idx:].reset_index(drop=True)
    
    print(f"\nTrain/test split:")
    print(f"  Train: {len(X_train):,} samples")
    print(f"  Test: {len(X_test):,} samples")
    _progress(f"prepare_training_data({disaster_type}) finished", phase0)
    
    return X_train, y_train, X_test, y_test, w_train, w_test


def train_model(
    disaster_type: str,
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    sample_weight_train: Optional[pd.Series] = None,
    use_smote: bool = False,
    custom_class_weights: Optional[Dict] = None,
    descriptor_string_encodings: Optional[Dict] = None,
) -> Tuple[RandomForestClassifier, Dict]:
    """Train a Random Forest model and return model + metadata.
    
    Args:
        use_smote: If True, use SMOTE to oversample minority classes
        custom_class_weights: Custom class weights dict (e.g., {0: 1, 1: 10, 2: 10, 3: 20})
    """
    print(f"\nTraining {disaster_type} model...")
    
    X_train_final = X_train.copy()
    y_train_final = y_train.copy()
    
    sample_weight_train_final = sample_weight_train.copy() if sample_weight_train is not None else None

    # v1.3: SMOTE disabled for all hazards (sample weights + class weights only).
    if use_smote:
        print("  Applying SMOTE/SMOTENC to oversample minority classes...")
        unique_classes = sorted(y_train.unique())
        if len(unique_classes) > 1:
            try:
                # Ensure labels are integers
                y_train_clean = y_train_final.astype(int)
                
                # Use SimpleImputer to handle NaN before SMOTE
                # Store original columns
                original_columns = X_train_final.columns.tolist()
                
                # Fill NaN with median (per column)
                X_train_imputed = X_train_final.copy()
                for col in X_train_imputed.columns:
                    if X_train_imputed[col].isna().any():
                        median_val = X_train_imputed[col].median()
                        if pd.isna(median_val):
                            X_train_imputed[col] = X_train_imputed[col].fillna(0)
                        else:
                            X_train_imputed[col] = X_train_imputed[col].fillna(median_val)
                
                # Convert to numpy for SMOTE
                X_train_array = X_train_imputed.values
                
                # Check k_neighbors (must be less than smallest class)
                min_class_size = min([(y_train_clean == c).sum() for c in unique_classes])
                k_neighbors = min(5, min_class_size - 1)
                if k_neighbors < 1:
                    k_neighbors = 1
                
                categorical_indices = [
                    i for i, col in enumerate(original_columns)
                    if col.startswith("soil_texture_class__")
                ]
                if categorical_indices:
                    sampler = SMOTENC(
                        categorical_features=categorical_indices,
                        random_state=RF_RANDOM_STATE,
                        k_neighbors=k_neighbors,
                    )
                    print(f"  Using SMOTENC with {len(categorical_indices)} categorical columns")
                else:
                    sampler = SMOTE(random_state=RF_RANDOM_STATE, k_neighbors=k_neighbors)
                    print("  Using SMOTE (no categorical columns detected)")
                X_train_final, y_train_final = sampler.fit_resample(X_train_array, y_train_clean)
                
                # Convert back to DataFrame to preserve column names
                X_train_final = pd.DataFrame(X_train_final, columns=original_columns)
                y_train_final = pd.Series(y_train_final, dtype=int)
                # Resampled points do not have reliable per-sample confidence provenance.
                sample_weight_train_final = None
                
                print(f"  After SMOTE: {len(X_train_final):,} samples")
                
                # Show new distribution
                print(f"  New label distribution:")
                for level in sorted(np.unique(y_train_final)):
                    count = (y_train_final == level).sum()
                    print(f"    Level {level}: {count:,} ({count/len(y_train_final)*100:.1f}%)")
            except Exception as e:
                print(f"  ⚠ SMOTE failed: {e}, continuing without SMOTE")
                import traceback
                traceback.print_exc()
                X_train_final = X_train
                y_train_final = y_train
    
    if disaster_type == "flood":
        class_weight = {0: 1, 1: 10, 2: 20, 3: 40}
    elif disaster_type == "landslide":
        class_weight = {0: 1, 1: 20, 2: 50}
    elif disaster_type in ("fire", "drought"):
        class_weight = "balanced"
    elif custom_class_weights is not None:
        class_weight = custom_class_weights
        print(f"  Using custom class weights: {class_weight}")
    else:
        class_weight = RF_CLASS_WEIGHT

    print(f"  class_weight: {class_weight}")

    model = RandomForestClassifier(
        n_estimators=RF_N_ESTIMATORS,
        max_depth=RF_MAX_DEPTH,
        min_samples_split=RF_MIN_SAMPLES_SPLIT,
        min_samples_leaf=RF_MIN_SAMPLES_LEAF,
        class_weight=class_weight,
        random_state=RF_RANDOM_STATE,
        n_jobs=-1,
    )
    
    if sample_weight_train_final is not None and len(sample_weight_train_final) == len(X_train_final):
        model.fit(X_train_final, y_train_final, sample_weight=sample_weight_train_final.values)
    else:
        model.fit(X_train_final, y_train_final)
    
    train_score = model.score(X_train, y_train)
    test_score = model.score(X_test, y_test)
    
    print(f"Train accuracy: {train_score:.4f}")
    print(f"Test accuracy: {test_score:.4f}")
    
    feature_importance = pd.Series(
        model.feature_importances_,
        index=X_train.columns
    ).sort_values(ascending=False)
    
    print(f"\nTop 10 most important features:")
    for i, (feat, imp) in enumerate(feature_importance.head(10).items(), 1):
        print(f"  {i}. {feat}: {imp:.4f}")
    
    n_classes = int(len(np.unique(y_train_final)))
    version = HAZARD_MODEL_VERSIONS.get(disaster_type, "1.3")
    metadata = {
        'disaster_type': disaster_type,
        'version': version,
        'training_date': datetime.now().isoformat(),
        'hyperparameters': {
            'n_estimators': RF_N_ESTIMATORS,
            'max_depth': RF_MAX_DEPTH,
            'min_samples_split': RF_MIN_SAMPLES_SPLIT,
            'min_samples_leaf': RF_MIN_SAMPLES_LEAF,
            'class_weight': class_weight if isinstance(class_weight, str) else dict(class_weight),
            'random_state': RF_RANDOM_STATE,
        },
        'training_config': {
            'used_smote': use_smote,
            'original_train_samples': len(X_train),
            'final_train_samples': len(X_train_final),
            'used_sample_weights': sample_weight_train is not None and sample_weight_train_final is not None,
        },
        'features': list(X_train.columns),
        'n_features': len(X_train.columns),
        'n_train_samples': len(X_train_final),
        'n_test_samples': len(X_test),
        'n_classes': n_classes,
        'train_accuracy': float(train_score),
        'test_accuracy': float(test_score),
        'feature_importance': {
            feat: float(imp) for feat, imp in feature_importance.items()
        },
        'descriptor_string_encodings': descriptor_string_encodings or {},
        'descriptor_one_hot_columns': [
            c for c in X_train.columns if c.startswith("soil_texture_class__")
        ],
    }
    if disaster_type == "landslide":
        metadata["label_schema"] = {
            "0": "none",
            "1": "mild",
            "2": "severe",
            "inference_high_confidence_level_3_threshold": 0.70,
        }
    
    return model, metadata


def save_model(
    model: RandomForestClassifier,
    metadata: Dict,
    disaster_type: str
) -> None:
    """Save model and metadata to disk."""
    MODELS_DIR.mkdir(exist_ok=True)
    
    version = HAZARD_MODEL_VERSIONS.get(disaster_type, "1.3")
    model_path = MODELS_DIR / f"{disaster_type}_model_v{version}.pkl"
    metadata_path = MODELS_DIR / f"{disaster_type}_model_v{version}_metadata.json"
    
    joblib.dump(model, model_path)
    print(f"Saved model: {model_path}")
    
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    print(f"Saved metadata: {metadata_path}")


def train_all_models(
    disaster_types: Optional[List[str]] = None,
    use_smote: Optional[Dict[str, bool]] = None,
    custom_class_weights: Optional[Dict[str, Dict]] = None,
) -> None:
    """Train models for all disaster types.

    v1.3: ``use_smote`` and ``custom_class_weights`` arguments are ignored; SMOTE is off for
    all hazards and class weights are fixed per hazard inside ``train_model``.
    """
    del use_smote, custom_class_weights  # API compatibility only (v1.3)
    if disaster_types is None:
        disaster_types = ['fire', 'drought', 'flood', 'landslide']

    use_smote = {'fire': False, 'drought': False, 'flood': False, 'landslide': False}

    MODELS_DIR.mkdir(exist_ok=True)
    desc_full = load_region_descriptors()
    descriptor_encodings = build_descriptor_encodings(desc_full)
    # Save a generic encodings file for the current default version (1.3)
    enc_path = MODELS_DIR / REGION_DESCRIPTOR_ENCODINGS_FILENAME.format(version="1.3")
    save_descriptor_encodings(descriptor_encodings, enc_path)
    print(f"Saved region descriptor encodings: {enc_path}")

    for disaster_type in disaster_types:
        try:
            X_train, y_train, X_test, y_test, w_train, w_test = prepare_training_data(
                disaster_type, descriptor_encodings=descriptor_encodings
            )
            
            model, metadata = train_model(
                disaster_type,
                X_train, y_train, X_test, y_test,
                sample_weight_train=w_train,
                use_smote=use_smote.get(disaster_type, False),
                custom_class_weights=None,
                descriptor_string_encodings=descriptor_encodings,
            )
            save_model(model, metadata, disaster_type)
            print(f"\n✓ {disaster_type} model training complete\n")
        except Exception as e:
            print(f"\n✗ Error training {disaster_type} model: {e}\n")
            raise


if __name__ == "__main__":
    train_all_models()
