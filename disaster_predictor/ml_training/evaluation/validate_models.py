"""
Evaluate trained models on test sets.

This module handles:
- Loading trained models
- Evaluating on test sets
- Computing precision, recall, F1 per risk level
- Computing weighted F1 and ROC-AUC
- Generating confusion matrices
- Feature importance analysis
"""

import sys
import time
from pathlib import Path
import pandas as pd
import numpy as np
import json
import joblib
from typing import Dict, List, Tuple, Optional
from sklearn.metrics import (
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix,
    roc_auc_score,
    classification_report,
)
from sklearn.model_selection import ParameterGrid

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from ml_training.config import (
    HAZARD_MODEL_VERSIONS,
    MODELS_DIR,
    CORRELATION_THRESHOLD,
)

VALIDATION_LOG = MODELS_DIR / f"validation_progress_v1.3.log"


def _log(msg: str) -> None:
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    try:
        MODELS_DIR.mkdir(parents=True, exist_ok=True)
        with open(VALIDATION_LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError:
        pass
from ml_training.models.train_models import (
    prepare_training_data,
    remove_correlated_features,
    get_feature_columns,
)


def load_model(disaster_type: str) -> Tuple:
    """Load model and metadata."""
    version = HAZARD_MODEL_VERSIONS.get(disaster_type, "1.3")
    model_path = MODELS_DIR / f"{disaster_type}_model_v{version}.pkl"
    metadata_path = MODELS_DIR / f"{disaster_type}_model_v{version}_metadata.json"
    
    if not model_path.exists():
        raise FileNotFoundError(f"Model not found: {model_path}")
    
    model = joblib.load(model_path)
    
    with open(metadata_path, 'r') as f:
        metadata = json.load(f)
    
    return model, metadata


def evaluate_model(
    disaster_type: str,
    model,
    X_test: pd.DataFrame,
    y_test: pd.Series
) -> Dict:
    """Evaluate model and return metrics."""
    y_pred = model.predict(X_test)
    
    # Get unique classes present in test set
    if isinstance(y_test, pd.Series):
        test_classes = set(y_test.unique())
    else:
        test_classes = set(np.unique(y_test))
    
    if isinstance(y_pred, pd.Series):
        pred_classes = set(y_pred.unique())
    else:
        pred_classes = set(np.unique(y_pred))
    
    unique_classes = sorted(test_classes | pred_classes)
    
    precision = precision_score(y_test, y_pred, labels=unique_classes, average=None, zero_division=0)
    recall = recall_score(y_test, y_pred, labels=unique_classes, average=None, zero_division=0)
    f1 = f1_score(y_test, y_pred, labels=unique_classes, average=None, zero_division=0)
    
    weighted_f1 = f1_score(y_test, y_pred, average='weighted', zero_division=0)
    
    # Create confusion matrix with all 4 levels, even if some are missing
    cm = confusion_matrix(y_test, y_pred, labels=[0, 1, 2, 3])
    
    y_binary = (y_test > 0).astype(int)
    y_pred_binary = (y_pred > 0).astype(int)
    
    try:
        roc_auc = roc_auc_score(y_binary, y_pred_binary)
    except ValueError:
        roc_auc = None
    
    # Map metrics to all 4 levels (use 0 for missing classes)
    precision_dict = {}
    recall_dict = {}
    f1_dict = {}
    
    for i in range(4):
        if i in unique_classes:
            idx = unique_classes.index(i)
            precision_dict[f'level_{i}'] = float(precision[idx])
            recall_dict[f'level_{i}'] = float(recall[idx])
            f1_dict[f'level_{i}'] = float(f1[idx])
        else:
            precision_dict[f'level_{i}'] = 0.0
            recall_dict[f'level_{i}'] = 0.0
            f1_dict[f'level_{i}'] = 0.0
    
    metrics = {
        'precision_per_class': precision_dict,
        'recall_per_class': recall_dict,
        'f1_per_class': f1_dict,
        'weighted_f1': float(weighted_f1),
        'roc_auc': float(roc_auc) if roc_auc is not None else None,
        'confusion_matrix': cm.tolist(),
    }
    
    return metrics


def _class_col_index(model) -> Dict[int, int]:
    return {int(c): i for i, c in enumerate(model.classes_)}


def _predict_from_proba_thresholded(
    proba: np.ndarray,
    col_idx: Dict[int, int],
    thresholds: Dict[str, float],
    disaster_type: str,
) -> np.ndarray:
    """L3 then L2 priority using calibrated thresholds; fallback argmax. Landslide: 3-class + UI L3."""
    t2 = float(thresholds.get("level_2", 0.5))
    t3 = float(thresholds.get("level_3", 0.5))
    n_cls = len(col_idx)
    i2 = col_idx.get(2)
    i3 = col_idx.get(3)
    preds = []
    for row in proba:
        if disaster_type == "landslide" and n_cls == 3 and i2 is not None:
            if row[i2] >= t3:
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
    return np.array(preds, dtype=int)


def _flood_threshold_score(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """v1.3: Score = (F1_L3 * 5) + (F1_L2 * 2) - (FPR_L0 * 1)."""
    y = np.asarray(y_true).astype(int)
    p = np.asarray(y_pred).astype(int)
    f1_l3 = f1_score(y == 3, p == 3, zero_division=0)
    f1_l2 = f1_score(y == 2, p == 2, zero_division=0)
    n0 = max(int(np.sum(y == 0)), 1)
    fpr_l0 = float(np.sum((y == 0) & (p > 0))) / n0
    return 5.0 * f1_l3 + 2.0 * f1_l2 - 1.0 * fpr_l0


def _landslide_threshold_score_l2(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """v1.3: Score = (F1_L2 * 5) - (FPR_L0 * 1)."""
    y = np.asarray(y_true).astype(int)
    p = np.asarray(y_pred).astype(int)
    f1_l2 = f1_score(y == 2, p == 2, zero_division=0)
    n0 = max(int(np.sum(y == 0)), 1)
    fpr_l0 = float(np.sum((y == 0) & (p > 0))) / n0
    return 5.0 * f1_l2 - 1.0 * fpr_l0


def calibrate_thresholds(
    model,
    X_cal: pd.DataFrame,
    y_cal: pd.Series,
    disaster_type: str,
) -> Dict[str, float]:
    """v1.3: Fire/drought fixed thresholds; flood/landslide grid with custom scores."""
    proba = model.predict_proba(X_cal)
    col_idx = _class_col_index(model)
    n_cls = len(col_idx)

    if disaster_type == "fire":
        best = {"level_2": 0.4, "level_3": 0.2}
        _log(f">>> [{disaster_type}] hardcoded thresholds (v1.3): {best}")
        return best

    if disaster_type == "drought":
        best = {"level_2": 0.4, "level_3": 0.4}
        _log(f">>> [{disaster_type}] hardcoded thresholds (v1.3): {best}")
        return best

    if disaster_type == "landslide" or n_cls == 3:
        t3_ui = 0.70
        grid2 = [0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55]
        best = {"level_2": 0.4, "level_3": t3_ui}
        best_score = -1e18
        for t2 in grid2:
            thr = {"level_2": t2, "level_3": t3_ui}
            pred = _predict_from_proba_thresholded(proba, col_idx, thr, "landslide")
            score = _landslide_threshold_score_l2(np.asarray(y_cal), pred)
            if score > best_score:
                best_score = score
                best = {"level_2": float(t2), "level_3": float(t3_ui)}
        _log(
            f">>> [{disaster_type}] calibrated L2 only (score=5*F1_L2-FPR_L0): "
            f"{best} (score={best_score:.4f})"
        )
        return best

    # flood (4-class)
    grid = list(
        ParameterGrid(
            {
                "level_2": [0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5],
                "level_3": [0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5],
            }
        )
    )
    best = {"level_2": 0.4, "level_3": 0.4}
    best_score = -1e18
    for cand in grid:
        pred = _predict_from_proba_thresholded(proba, col_idx, cand, disaster_type)
        score = _flood_threshold_score(np.asarray(y_cal), pred)
        if score > best_score:
            best_score = score
            best = {"level_2": float(cand["level_2"]), "level_3": float(cand["level_3"])}
    _log(
        f">>> [{disaster_type}] grid thresholds (score=5*F1_L3+2*F1_L2-FPR_L0): "
        f"{best} (score={best_score:.4f})"
    )
    return best


def evaluate_model_with_thresholds(
    model,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    thresholds: Dict[str, float],
    disaster_type: str,
) -> Dict:
    proba = model.predict_proba(X_test)
    col_idx = _class_col_index(model)
    y_pred = _predict_from_proba_thresholded(proba, col_idx, thresholds, disaster_type)
    unique_classes = sorted(set(np.unique(y_test)) | set(np.unique(y_pred)))
    precision = precision_score(y_test, y_pred, labels=unique_classes, average=None, zero_division=0)
    recall = recall_score(y_test, y_pred, labels=unique_classes, average=None, zero_division=0)
    f1 = f1_score(y_test, y_pred, labels=unique_classes, average=None, zero_division=0)
    weighted_f1 = f1_score(y_test, y_pred, average='weighted', zero_division=0)
    cm = confusion_matrix(y_test, y_pred, labels=[0, 1, 2, 3])
    y_binary = (y_test > 0).astype(int)
    y_pred_binary = (y_pred > 0).astype(int)
    try:
        roc_auc = roc_auc_score(y_binary, y_pred_binary)
    except ValueError:
        roc_auc = None
    precision_dict = {}
    recall_dict = {}
    f1_dict = {}
    for i in range(4):
        if i in unique_classes:
            j = unique_classes.index(i)
            precision_dict[f'level_{i}'] = float(precision[j])
            recall_dict[f'level_{i}'] = float(recall[j])
            f1_dict[f'level_{i}'] = float(f1[j])
        else:
            precision_dict[f'level_{i}'] = 0.0
            recall_dict[f'level_{i}'] = 0.0
            f1_dict[f'level_{i}'] = 0.0
    return {
        'precision_per_class': precision_dict,
        'recall_per_class': recall_dict,
        'f1_per_class': f1_dict,
        'weighted_f1': float(weighted_f1),
        'roc_auc': float(roc_auc) if roc_auc is not None else None,
        'confusion_matrix': cm.tolist(),
    }


def print_evaluation_results(disaster_type: str, metrics: Dict) -> None:
    """Print evaluation results in readable format."""
    print(f"\n{'='*60}")
    print(f"Evaluation Results: {disaster_type.upper()}")
    print(f"{'='*60}")
    
    print("\nPer-Class Metrics:")
    print(f"{'Level':<10} {'Precision':<12} {'Recall':<12} {'F1-Score':<12}")
    print("-" * 50)
    
    for i in range(4):
        prec = metrics['precision_per_class'][f'level_{i}']
        rec = metrics['recall_per_class'][f'level_{i}']
        f1 = metrics['f1_per_class'][f'level_{i}']
        print(f"{i:<10} {prec:<12.4f} {rec:<12.4f} {f1:<12.4f}")
    
    print(f"\nOverall Metrics:")
    print(f"  Weighted F1: {metrics['weighted_f1']:.4f}")
    if metrics['roc_auc'] is not None:
        print(f"  ROC-AUC: {metrics['roc_auc']:.4f}")
    
    print(f"\nConfusion Matrix:")
    print(f"        Predicted")
    print(f"        0    1    2    3")
    cm = np.array(metrics['confusion_matrix'])
    for i, row in enumerate(cm):
        print(f"  {i}   {row[0]:4d} {row[1]:4d} {row[2]:4d} {row[3]:4d}")


def validate_all_models(disaster_types: Optional[List[str]] = None) -> Dict:
    """Validate all models and return results."""
    if disaster_types is None:
        disaster_types = ['fire', 'drought', 'flood', 'landslide']
    
    results = {}
    overall = time.perf_counter()
    try:
        VALIDATION_LOG.unlink(missing_ok=True)
    except OSError:
        pass
    _log(f"=== validate_all_models start | HAZARD_MODEL_VERSIONS={HAZARD_MODEL_VERSIONS} | hazards={disaster_types} ===")
    _log("Each hazard rebuilds train/test from BigQuery (slow). Progress lines below are live.")
    
    for i, disaster_type in enumerate(disaster_types):
        try:
            loop_start = time.perf_counter()
            print(f"\n{'='*60}", flush=True)
            print(f"Validating {disaster_type} model ({i + 1}/{len(disaster_types)})...", flush=True)
            print(f"{'='*60}", flush=True)
            _log(f">>> [{i + 1}/{len(disaster_types)}] {disaster_type}: loading .pkl + metadata...")
            
            model, metadata = load_model(disaster_type)
            _log(f">>> [{disaster_type}] model loaded; rebuilding test set from BQ (often 5–15+ min)...")
            X_train, y_train, X_test, y_test, w_train, w_test = prepare_training_data(disaster_type)
            _log(f">>> [{disaster_type}] data ready: test n={len(X_test):,} (+{time.perf_counter() - loop_start:.1f}s)")
            
            # Align test features with those used at training time
            feature_cols = metadata.get("features")
            if feature_cols:
                # Keep only columns that the model was trained on
                missing_in_test = [c for c in feature_cols if c not in X_test.columns]
                if missing_in_test:
                    print(f"Warning: {len(missing_in_test)} training features missing in X_test. "
                          f"Filling them with zeros.", flush=True)
                    for col in missing_in_test:
                        X_test[col] = 0.0
                # Reorder columns to match training order
                X_test = X_test[feature_cols]

            # Chronological holdout: first half of the 20% test tail = validation (calibration);
            # second half = final test metrics (~10% + ~10% of full series).
            n_hold = len(X_test)
            mid = max(1, n_hold // 2)
            X_val = X_test.iloc[:mid].copy()
            y_val = y_test.iloc[:mid].copy()
            X_final = X_test.iloc[mid:].copy()
            y_final = y_test.iloc[mid:].copy()
            _log(
                f">>> [{disaster_type}] temporal holdout: calib n={len(X_val):,}, "
                f"final_test n={len(X_final):,}"
            )

            _log(f">>> [{disaster_type}] calibrating thresholds on validation slice (not train)...")
            thresholds = calibrate_thresholds(model, X_val, y_val, disaster_type)
            metadata["class_thresholds"] = thresholds
            calib_meta = {
                "split": "first_half_of_chronological_test_for_calib",
            }
            if disaster_type == "fire":
                calib_meta["metric"] = "hardcoded_v1_3_fire"
            elif disaster_type == "drought":
                calib_meta["metric"] = "hardcoded_v1_3_drought"
            elif disaster_type == "flood":
                calib_meta["metric"] = "5_f1_l3_plus_2_f1_l2_minus_fpr_l0"
            elif disaster_type == "landslide":
                calib_meta["metric"] = "5_f1_l2_minus_fpr_l0_level3_ui_fixed_0_70"
            else:
                calib_meta["metric"] = "unknown"
            metadata["threshold_calibration"] = calib_meta
            version = HAZARD_MODEL_VERSIONS.get(disaster_type, "1.3")
            metadata_path = MODELS_DIR / f"{disaster_type}_model_v{version}_metadata.json"
            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2)
            _log(f">>> [{disaster_type}] thresholds saved to metadata")

            _log(f">>> [{disaster_type}] running predict_proba + thresholded metrics (final test)...")
            metrics = evaluate_model_with_thresholds(model, X_final, y_final, thresholds, disaster_type)
            print_evaluation_results(disaster_type, metrics)
            
            results[disaster_type] = {
                'metadata': metadata,
                'metrics': metrics,
            }
            
            _log(
                f">>> [{disaster_type}] DONE weighted_f1={metrics['weighted_f1']:.4f} "
                f"roc_auc={metrics.get('roc_auc')} "
                f"step_total={time.perf_counter() - loop_start:.1f}s "
                f"cumulative={time.perf_counter() - overall:.1f}s"
            )
            print(f"\n✓ {disaster_type} validation complete", flush=True)
            
        except FileNotFoundError as e:
            print(f"\n✗ Model not found for {disaster_type}: {e}")
        except Exception as e:
            _log(f"!!! ERROR {disaster_type}: {e}")
            print(f"\n✗ Error validating {disaster_type}: {e}", flush=True)
            raise
    
    _log(f"=== validate_all_models finished in {time.perf_counter() - overall:.1f}s ===")
    return results


if __name__ == "__main__":
    validate_all_models()
