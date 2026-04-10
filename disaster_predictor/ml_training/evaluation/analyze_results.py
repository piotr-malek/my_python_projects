"""
Deep analysis of model performance with benchmarks and recommendations.
"""

import sys
from pathlib import Path
import json
import pandas as pd
import numpy as np
from typing import Dict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from ml_training.config import MODELS_DIR, MODEL_VERSION


def load_validation_results() -> Dict:
    """Load validation results."""
    results_path = MODELS_DIR / "validation_results.json"
    if not results_path.exists():
        raise FileNotFoundError(f"Validation results not found: {results_path}")
    
    with open(results_path, 'r') as f:
        return json.load(f)


def analyze_model_performance(results: Dict) -> None:
    """Analyze model performance with benchmarks and recommendations."""
    
    print("\n" + "="*80)
    print("COMPREHENSIVE MODEL PERFORMANCE ANALYSIS")
    print("="*80)
    
    benchmarks = {
        'excellent': {'f1': 0.90, 'precision': 0.85, 'recall': 0.85},
        'good': {'f1': 0.75, 'precision': 0.70, 'recall': 0.70},
        'acceptable': {'f1': 0.60, 'precision': 0.55, 'recall': 0.55},
        'poor': {'f1': 0.50, 'precision': 0.45, 'recall': 0.45},
    }
    
    for disaster_type, data in results.items():
        print(f"\n{'='*80}")
        print(f"{disaster_type.upper()} MODEL ANALYSIS")
        print(f"{'='*80}")
        
        metrics = data['metrics']
        metadata = data['metadata']
        
        # Overall metrics
        weighted_f1 = metrics['weighted_f1']
        roc_auc = metrics.get('roc_auc')
        
        print(f"\n📊 OVERALL PERFORMANCE:")
        print(f"  Weighted F1-Score: {weighted_f1:.4f}")
        if roc_auc:
            print(f"  ROC-AUC: {roc_auc:.4f}")
        print(f"  Test Accuracy: {metadata['test_accuracy']:.4f}")
        
        # Benchmark comparison
        if weighted_f1 >= benchmarks['excellent']['f1']:
            benchmark = "EXCELLENT"
        elif weighted_f1 >= benchmarks['good']['f1']:
            benchmark = "GOOD"
        elif weighted_f1 >= benchmarks['acceptable']['f1']:
            benchmark = "ACCEPTABLE"
        else:
            benchmark = "NEEDS IMPROVEMENT"
        
        print(f"\n🎯 BENCHMARK: {benchmark}")
        print(f"   (Compared to typical disaster prediction models)")
        
        # Per-class analysis
        print(f"\n📈 PER-CLASS PERFORMANCE:")
        print(f"{'Level':<8} {'Precision':<12} {'Recall':<12} {'F1':<12} {'Status':<15}")
        print("-" * 70)
        
        class_issues = []
        for level in [0, 1, 2, 3]:
            prec = metrics['precision_per_class'][f'level_{level}']
            rec = metrics['recall_per_class'][f'level_{level}']
            f1 = metrics['f1_per_class'][f'level_{level}']
            
            # Determine status
            if f1 >= 0.80:
                status = "✓ Excellent"
            elif f1 >= 0.60:
                status = "✓ Good"
            elif f1 >= 0.40:
                status = "⚠ Acceptable"
            else:
                status = "✗ Poor"
                class_issues.append(level)
            
            print(f"{level:<8} {prec:<12.4f} {rec:<12.4f} {f1:<12.4f} {status:<15}")
        
        # Confusion matrix analysis
        cm = np.array(metrics['confusion_matrix'])
        print(f"\n🔍 CONFUSION MATRIX ANALYSIS:")
        
        total = cm.sum()
        correct = np.trace(cm)
        print(f"  Overall accuracy: {correct/total:.2%}")
        
        # Check for common misclassification patterns
        misclassifications = []
        for true_level in range(4):
            for pred_level in range(4):
                if true_level != pred_level and cm[true_level, pred_level] > 0:
                    count = cm[true_level, pred_level]
                    pct = count / cm[true_level].sum() * 100
                    if pct > 10:  # More than 10% misclassification
                        misclassifications.append(
                            (true_level, pred_level, count, pct)
                        )
        
        if misclassifications:
            print(f"\n  ⚠ Common misclassifications (>10%):")
            for true_lvl, pred_lvl, count, pct in sorted(misclassifications, key=lambda x: x[3], reverse=True):
                print(f"    Level {true_lvl} → Level {pred_lvl}: {count} cases ({pct:.1f}%)")
        else:
            print(f"  ✓ No significant misclassification patterns")
        
        # Class imbalance analysis
        print(f"\n⚖️  CLASS IMBALANCE ANALYSIS:")
        train_samples = metadata['n_train_samples']
        test_samples = metadata['n_test_samples']
        
        # Estimate class distribution from confusion matrix
        class_counts = cm.sum(axis=1)
        class_pcts = class_counts / class_counts.sum() * 100
        
        print(f"  Test set class distribution:")
        for level in range(4):
            count = class_counts[level]
            pct = class_pcts[level]
            print(f"    Level {level}: {count:,} ({pct:.1f}%)")
        
        imbalance_ratio = class_counts.max() / class_counts[class_counts > 0].min()
        print(f"\n  Imbalance ratio: {imbalance_ratio:.1f}:1")
        
        if imbalance_ratio > 10:
            print(f"  ⚠ Severe class imbalance detected")
        elif imbalance_ratio > 5:
            print(f"  ⚠ Moderate class imbalance")
        else:
            print(f"  ✓ Relatively balanced")
        
        # Feature importance insights
        print(f"\n🔑 TOP FEATURES:")
        feature_imp = metadata['feature_importance']
        top_features = sorted(feature_imp.items(), key=lambda x: x[1], reverse=True)[:5]
        for i, (feat, imp) in enumerate(top_features, 1):
            print(f"  {i}. {feat}: {imp:.4f}")
        
        # Recommendations
        print(f"\n💡 RECOMMENDATIONS:")
        recommendations = []
        
        if weighted_f1 < benchmarks['good']['f1']:
            recommendations.append("Overall performance below 'good' benchmark - consider improvements")
        
        if class_issues:
            for level in class_issues:
                if level == 0:
                    recommendations.append(f"Level 0 (no risk) performance poor - may indicate over-prediction")
                else:
                    recommendations.append(f"Level {level} (risk) performance poor - minority class issue")
        
        if imbalance_ratio > 10:
            recommendations.append("Severe class imbalance - consider SMOTE or class weighting adjustments")
        
        if len(misclassifications) > 3:
            recommendations.append("Multiple misclassification patterns - consider feature engineering")
        
        # Check if Level 1 has issues (common in disaster prediction)
        if 1 in class_issues:
            recommendations.append("Level 1 (low risk) struggling - consider merging with Level 2 or adjusting thresholds")
        
        if not recommendations:
            recommendations.append("✓ Model performance is good - no major issues identified")
        
        for i, rec in enumerate(recommendations, 1):
            print(f"  {i}. {rec}")
        
        # Specific improvement strategies
        if class_issues or weighted_f1 < benchmarks['good']['f1']:
            print(f"\n🔧 SPECIFIC IMPROVEMENT STRATEGIES:")
            
            if 1 in class_issues:
                print(f"  • Level 1 improvements:")
                print(f"    - Consider merging Level 1 and Level 2 into single 'moderate risk' class")
                print(f"    - Adjust label creation thresholds to better separate Level 1 from Level 0")
                print(f"    - Use cost-sensitive learning (higher penalty for missing Level 1)")
            
            if imbalance_ratio > 10:
                print(f"  • Class imbalance solutions:")
                print(f"    - Use SMOTE to oversample minority classes")
                print(f"    - Adjust class_weight parameter (currently: {metadata['hyperparameters']['class_weight']})")
                print(f"    - Use stratified sampling in training")
            
            if weighted_f1 < 0.70:
                print(f"  • General improvements:")
                print(f"    - Feature engineering: Add more temporal features (e.g., 14-day, 30-day trends)")
                print(f"    - Hyperparameter tuning: Grid search for n_estimators, max_depth")
                print(f"    - Ensemble methods: Combine multiple models")
                print(f"    - More training data: Add more regions or extend date range")


def compare_all_models(results: Dict) -> None:
    """Compare all models side-by-side."""
    print(f"\n{'='*80}")
    print("MODEL COMPARISON SUMMARY")
    print(f"{'='*80}")
    
    comparison_data = []
    for disaster_type, data in results.items():
        metrics = data['metrics']
        metadata = data['metadata']
        
        comparison_data.append({
            'Disaster': disaster_type.upper(),
            'Weighted F1': metrics['weighted_f1'],
            'ROC-AUC': metrics.get('roc_auc', 0),
            'Test Accuracy': metadata['test_accuracy'],
            'Train Samples': metadata['n_train_samples'],
            'Test Samples': metadata['n_test_samples'],
            'Features': metadata['n_features'],
        })
    
    df = pd.DataFrame(comparison_data)
    
    print("\n" + df.to_string(index=False))
    
    print(f"\n📊 RANKINGS:")
    print(f"  Best Weighted F1: {df.loc[df['Weighted F1'].idxmax(), 'Disaster']} ({df['Weighted F1'].max():.4f})")
    print(f"  Best ROC-AUC: {df.loc[df['ROC-AUC'].idxmax(), 'Disaster']} ({df['ROC-AUC'].max():.4f})")
    print(f"  Best Accuracy: {df.loc[df['Test Accuracy'].idxmax(), 'Disaster']} ({df['Test Accuracy'].max():.4f})")


if __name__ == "__main__":
    results = load_validation_results()
    analyze_model_performance(results)
    compare_all_models(results)
