{% macro compute_z_value(arrival, hist_avg, hist_sd) -%}
SAFE_DIVIDE({{ arrival }} - {{ hist_avg }}, NULLIF({{ hist_sd }}, 0))
{%- endmacro %}

{% macro flag_from_z(z_value, hist_sd) -%}
CASE
  WHEN {{ hist_sd }} IS NULL THEN NULL
  WHEN {{ hist_sd }} = 0 THEN 0
  WHEN ABS({{ z_value }}) >= 2 THEN 2 * SIGN({{ z_value }})
  WHEN ABS({{ z_value }}) >= 1 THEN 1 * SIGN({{ z_value }})
  ELSE 0
END
{%- endmacro %}

{% macro ratio(arrival, hist_avg) -%}
SAFE_DIVIDE({{ arrival }}, {{ hist_avg }})
{%- endmacro %}

{% macro delta(arrival, hist_avg) -%}
({{ arrival }} - {{ hist_avg }})
{%- endmacro %}


