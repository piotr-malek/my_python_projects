#!/usr/bin/env python3
"""
Metric name mappings for LLM readability.
Converts technical metric field names to clear, unambiguous descriptions.
"""

# Mapping from metric field names to clear, unambiguous descriptions
METRIC_NAME_MAP = {
    "sm1_mean": "Soil Moisture Layer 1 (0-7cm depth)",
    "sm2_mean": "Soil Moisture Layer 2 (7-28cm depth)",
    "spi30": "Standardized Precipitation Index (30-day period)",
    "ndvi_mean": "Vegetation Index (NDVI - Normalized Difference Vegetation Index)",
    "precipitation_7d_low_days": "Days with Low Precipitation (past 7 days)",
    "temp_2m_mean_C": "Air Temperature at 2m height (Celsius)",
    "frp_mean": "Fire Radiative Power (MW - Megawatts)",
}

# Mapping for internal metric fields to readable names
METRIC_FIELD_MAP = {
    "current": "Current Value",
    "climatology_p10": "Historical 10th Percentile",
    "climatology_p20": "Historical 20th Percentile",
    "climatology_p80": "Historical 80th Percentile",
    "climatology_p95": "Historical 95th Percentile",
    "climatology_frp_p95": "Historical 95th Percentile for Fire Radiative Power",
    "climatology_hotspot_p95": "Historical 95th Percentile for Hotspot Count",
    "threshold_mild": "Mild Drought Threshold",
    "hotspot_count": "Active Fire Hotspot Count",
    "previous": "Previous Value",
    "previous_date": "Previous Measurement Date",
    "change_pct": "Percentage Change from Previous",
}


def map_metrics_for_llm(key_metrics):
    """
    Convert metric names and field names to clear, unambiguous descriptions.
    
    Args:
        key_metrics: Dictionary with metric names as keys and metric data as values
        
    Returns:
        Dictionary with mapped metric names and field names
    """
    if not key_metrics:
        return {}
    
    mapped = {}
    for metric_key, metric_data in key_metrics.items():
        # Map metric name
        readable_name = METRIC_NAME_MAP.get(metric_key, metric_key.replace("_", " ").title())
        
        # Map internal fields
        mapped_data = {}
        for field_key, field_value in metric_data.items():
            readable_field = METRIC_FIELD_MAP.get(field_key, field_key.replace("_", " ").title())
            mapped_data[readable_field] = field_value
        
        mapped[readable_name] = mapped_data
    
    return mapped
