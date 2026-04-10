"""
Format assessment results for LLM interpretation.

Converts technical assessment data into human-readable explanations.
"""

from typing import Dict, List, Optional
import pandas as pd


RISK_LEVEL_NAMES = {
    0: "no risk",
    1: "low risk",
    2: "medium risk",
    3: "high risk"
}


def format_for_llm(
    disaster_type: str,
    region: str,
    date: pd.Timestamp,
    assessment_details: Dict,
    previous_risk: Optional[int] = None
) -> Dict:
    """
    Format assessment results for LLM interpretation.
    
    Args:
        disaster_type: 'fire', 'drought', 'flood', or 'landslide'
        region: Region identifier
        date: Assessment date
        assessment_details: Output from MLDisasterDetection.assess_risk()
        previous_risk: Previous risk level (if available) for change detection
    
    Returns:
        Dictionary with formatted information for LLM
    """
    risk_level = assessment_details.get('final_risk', 0)
    ml_prediction = assessment_details.get('ml_prediction', 0)
    
    # Collect contributing factors
    factors = []
    
    # ML prediction (only rating available, no internal reasoning)
    ml_info = {}
    if ml_prediction > 0:
        ml_info = {
            'type': 'ml_model',
            'ml_prediction': ml_prediction,
            'ml_prediction_name': RISK_LEVEL_NAMES.get(ml_prediction, f'risk level {ml_prediction}')
        }
    
    # Recent outlook: metric -> { percentile_approx, value?, unit? }
    recent_outlook = assessment_details.get('recent_outlook') or {}
    climatology_info = {'type': 'climatology'}
    if isinstance(recent_outlook, dict):
        for metric_key, entry in recent_outlook.items():
            if isinstance(entry, dict) and 'percentile_approx' in entry:
                pct = entry.get('percentile_approx')
                val = entry.get('value')
                unit = entry.get('unit', '')
                if val is not None and unit:
                    climatology_info[metric_key] = f"p{pct:.0f}, {val}{unit}"
                else:
                    climatology_info[metric_key] = f"p{pct:.0f}"
    
    # Forecast outlook: same shape as recent_outlook
    forecast_outlook = assessment_details.get('forecast_outlook') or {}
    forecast_info = {'type': 'forecast'}
    if isinstance(forecast_outlook, dict):
        for metric_key, entry in forecast_outlook.items():
            if isinstance(entry, dict) and 'percentile_approx' in entry:
                pct = entry.get('percentile_approx')
                val = entry.get('value')
                unit = entry.get('unit', '')
                if val is not None and unit:
                    forecast_info[metric_key] = f"p{pct:.0f}, {val}{unit}"
                else:
                    forecast_info[metric_key] = f"p{pct:.0f}"
    
    # Collect all factors
    if ml_info:
        factors.append(ml_info)
    if len(climatology_info) > 1:  # More than just 'type'
        factors.append(climatology_info)
    if len(forecast_info) > 1:  # More than just 'type'
        factors.append(forecast_info)
    
    # Determine change
    change = None
    if previous_risk is not None:
        if risk_level > previous_risk:
            change = 'increased'
        elif risk_level < previous_risk:
            change = 'decreased'
        else:
            change = 'unchanged'
    
    # Build explanation
    explanation_parts = []
    
    if change:
        explanation_parts.append(f"Risk level {change} from {RISK_LEVEL_NAMES.get(previous_risk, 'unknown')} to {RISK_LEVEL_NAMES.get(risk_level, 'unknown')}")
    else:
        explanation_parts.append(f"Risk level is {RISK_LEVEL_NAMES.get(risk_level, 'unknown')}")
    
    # Add key factors (raw comparisons)
    key_factors = []
    for f in factors[:3]:  # Top 3 factors
        if f['type'] == 'ml_model':
            key_factors.append(f"ML model: {f.get('ml_prediction_name', 'risk level ' + str(f.get('ml_prediction', '?')))}")
        elif f['type'] == 'climatology':
            for k, v in f.items():
                if k != 'type' and isinstance(v, str):
                    key_factors.append(f"{k}: {v}")
        elif f['type'] == 'forecast':
            for k, v in f.items():
                if k != 'type' and isinstance(v, str):
                    key_factors.append(f"{k}: {v}")
    
    if key_factors:
        explanation_parts.append("due to: " + ", ".join(key_factors[:5]))  # Limit to 5
    
    explanation = ". ".join(explanation_parts) + "."
    
    return {
        'disaster_type': disaster_type,
        'region': region,
        'date': date.strftime('%Y-%m-%d'),
        'risk_level': risk_level,
        'risk_level_name': RISK_LEVEL_NAMES.get(risk_level, 'unknown'),
        'ml_prediction': ml_prediction,
        'previous_risk': previous_risk,
        'change': change,
        'factors': factors,
        'explanation': explanation,
    }


def generate_llm_prompt(
    llm_data: Dict,
    style: str = 'concise'
) -> str:
    """
    Generate LLM prompt for risk explanation.
    
    Args:
        llm_data: Output from format_for_llm()
        style: 'concise' (1 sentence) or 'detailed' (paragraph)
    
    Returns:
        Formatted prompt string
    """
    disaster_type = llm_data['disaster_type']
    region = llm_data['region']
    date = llm_data['date']
    risk_level = llm_data['risk_level_name']
    factors = llm_data['factors']
    
    if style == 'concise':
        # One sentence explanation
        factor_lines = []
        for f in factors:
            if f['type'] == 'ml_model':
                factor_lines.append(f"- ML model: {f.get('ml_prediction_name', 'risk level ' + str(f.get('ml_prediction', '?')))}")
            elif f['type'] == 'climatology':
                for k, v in f.items():
                    if k != 'type' and isinstance(v, str):
                        factor_lines.append(f"- {v}")
            elif f['type'] == 'forecast':
                for k, v in f.items():
                    if k != 'type' and isinstance(v, str):
                        factor_lines.append(f"- {v}")
        
        prompt = f"""Generate a concise one-sentence explanation for {disaster_type} risk assessment.

Region: {region}
Date: {date}
Risk Level: {risk_level}

Factors:
{chr(10).join(factor_lines)}

Generate a single sentence explaining why the risk level is {risk_level}, incorporating the factors naturally."""
    
    else:
        # Detailed paragraph
        factor_lines = []
        for f in factors:
            if f['type'] == 'ml_model':
                factor_lines.append(f"- ML model: {f.get('ml_prediction_name', 'risk level ' + str(f.get('ml_prediction', '?')))}")
            elif f['type'] == 'climatology':
                for k, v in f.items():
                    if k != 'type' and isinstance(v, str):
                        factor_lines.append(f"- Climatology: {v}")
            elif f['type'] == 'forecast':
                for k, v in f.items():
                    if k != 'type' and isinstance(v, str):
                        factor_lines.append(f"- Forecast: {v}")
        
        prompt = f"""Generate a detailed explanation for {disaster_type} risk assessment.

Region: {region}
Date: {date}
Risk Level: {risk_level}

Factors:
{chr(10).join(factor_lines)}

Generate a 2-3 sentence explanation that:
1. States the current risk level
2. Explains the primary contributing factors
3. Provides context about why these factors matter for {disaster_type} risk"""
    
    return prompt


if __name__ == "__main__":
    # Example usage
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    
    from risk_assessment.ml_detection import MLDisasterDetection
    import pandas as pd
    import json
    
    detection = MLDisasterDetection()
    region = 'Lake_Titicaca_Peru_01'
    date = pd.to_datetime('2024-07-15')
    
    # Get assessment
    risk_level, details = detection.assess_risk('fire', region, date)
    
    # Format for LLM
    llm_data = format_for_llm('fire', region, date, details, previous_risk=1)
    
    print("=" * 80)
    print("LLM INTERPRETATION DATA")
    print("=" * 80)
    print(json.dumps(llm_data, indent=2))
    
    print()
    print("=" * 80)
    print("LLM PROMPT (CONCISE)")
    print("=" * 80)
    print(generate_llm_prompt(llm_data, style='concise'))
    
    print()
    print("=" * 80)
    print("EXPLANATION (AUTO-GENERATED)")
    print("=" * 80)
    print(llm_data['explanation'])
