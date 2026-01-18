WITH inputs AS (
  SELECT
    bird,
    arrival_year,
    location_name,
    -- z-like anomaly scores in roughly [-2, 2]
    tmean_score,
    tmin_score,
    tmax_score,
    precip_score,
    srad_score,
    warm10_score,
    warm15_score,
    warm20_score,
    warm_anomaly_score,
    optimal_insect_score
  FROM {{ ref('mart_insect_metrics_condensed') }}
),

calc AS (
  SELECT
    bird,
    arrival_year,
    location_name,

    -- -------- Thermal latent (z_thermal_raw) --------
    -- Soft-min of (tmin, tmean) to reflect the bottleneck of cold nights/low means.
    -- softmin_k(a,b) = -log(exp(-k*a) + exp(-k*b)) / k, with k=1 here.
    -- Cap tmax a bit so extreme highs don't dominate (saturation).
    (
      0.40 * ( -LOG(EXP(-1.0 * tmin_score) + EXP(-1.0 * tmean_score)) / 1.0 ) +
      0.15 * LEAST(tmax_score, 1.5) +
      0.10 * GREATEST(warm10_score, 0.0) +
      0.15 * GREATEST(warm15_score, 0.0) +
      0.10 * GREATEST(warm20_score, 0.0) +
      0.10 * warm_anomaly_score
    ) AS z_thermal_raw,

    -- Radiation (monotonic)
    srad_score,

    -- Moisture U-shaped preference:
    -- Gaussian centered at a slight positive anomaly (mu ~ +0.5),
    -- sigma ~ 1.1. Returns [0,1], near 1 when precip ~ +0.5σ, lower for very dry or very wet.
    EXP( -POW( (precip_score - 0.5) / 1.1, 2 ) ) AS p_precip_pref,

    -- Observational gate (optimal insect days)
    optimal_insect_score
  FROM inputs
),

probs AS (
  SELECT
    bird,
    arrival_year,
    location_name,

    -- Convert latent z-scores to "probabilities" via logistic (k ~ 1.1)
    1.0 / (1.0 + EXP(-1.1 * z_thermal_raw))     AS p_thermal,
    1.0 / (1.0 + EXP(-1.1 * srad_score))        AS p_srad,
    p_precip_pref                               AS p_precip,   -- already in [0,1]
    1.0 / (1.0 + EXP(-1.1 * optimal_insect_score)) AS p_optimal
  FROM calc
),

combine AS (
  SELECT
    bird,
    arrival_year,
    location_name,

    -- Bottleneck-style aggregation across pillars
    POW(GREATEST(1e-6, p_thermal * p_srad * p_precip), 1.0/3.0) AS p_core,
    p_optimal
  FROM probs
),

final AS (
  SELECT
    bird,
    arrival_year,
    location_name,
    -- Blend physical suitability with the empirical "optimal" gate
    0.6 * p_core + 0.4 * p_optimal AS p_final
  FROM combine
)

SELECT
  bird,
  arrival_year,
  location_name,
  -- Map probability to a familiar [-2, 2] scale (centered at 0)
  4.0 * (p_final - 0.5) AS insect_presence_score
FROM final