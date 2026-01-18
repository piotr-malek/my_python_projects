import pandas as pd
from bigquery import load_from_bigquery
from dotenv import load_dotenv
import os
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LassoCV

load_dotenv()

features_all = load_from_bigquery(f"""
SELECT *
FROM `{os.getenv("BQ_PROJECT_ID")}.dbt_marts.mart_bird_year_features_all`
""")

birds = features_all['bird'].unique()
metric_columns = features_all.columns[3:35]

correlations = []
for bird in birds:
    bird_data = features_all[features_all['bird'] == bird]
    
    for metric in metric_columns:
        if bird_data[metric].nunique() <= 1:
            continue
            
        spearman = bird_data[metric].corr(bird_data['arrival_z_score'], method='spearman')
        pearson = bird_data[metric].corr(bird_data['arrival_z_score'], method='pearson')
        
        correlations.append({
            'bird': bird,
            'metric': metric,
            'spearman_corr': spearman,
            'pearson_corr': pearson
        })

correlations_df = pd.DataFrame(correlations)

def calculate_lasso_regression(bird_data, features, target='arrival_z_score'):
    X = bird_data[features].copy()
    y = bird_data[target].copy()
    # Drop rows with NA in X or y
    mask = X.notnull().all(axis=1) & y.notnull()
    X = X.loc[mask]
    y = y.loc[mask]
    if X.shape[0] < 2:
        # Not enough data points for regression
        return {feat: 0.0 for feat in features}
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    lasso = LassoCV(cv=5, random_state=0).fit(X_scaled, y)
    coefs = dict(zip(features, lasso.coef_))
    return coefs

# Calculate lasso coefficients per bird for top metrics
lasso_coefs_dict = {}
for bird in birds:
    bird_corrs = correlations_df[correlations_df['bird'] == bird]
    # Select top metrics by absolute pearson correlation, at most 10 metrics
    top_metrics = bird_corrs.reindex(bird_corrs['pearson_corr'].abs().sort_values(ascending=False).index)['metric'].head(10).tolist()
    bird_data = features_all[features_all['bird'] == bird]
    coefs = calculate_lasso_regression(bird_data, top_metrics)
    lasso_coefs_dict[bird] = coefs

# Add lasso_coef column to correlations_df
def get_lasso_coef(row):
    bird = row['bird']
    metric = row['metric']
    return lasso_coefs_dict.get(bird, {}).get(metric, 0.0)

correlations_df['lasso_coef'] = correlations_df.apply(get_lasso_coef, axis=1)

# For each bird, sort and print top correlations by lasso_coef (highest and lowest)
for bird in birds:
    bird_corrs = correlations_df[correlations_df['bird'] == bird].copy()
    if bird_corrs.empty:
        print(f"\nNo metrics for {bird}")
        continue

    # Sort descending by lasso, then pearson, then spearman
    bird_corrs.sort_values(by=['lasso_coef', 'pearson_corr', 'spearman_corr'], ascending=[False, False, False], inplace=True)
    top_high = bird_corrs.head(5)

    # Bottom (most negative lasso) — sort ascending by the same keys
    bottom_low = bird_corrs.sort_values(by=['lasso_coef', 'pearson_corr', 'spearman_corr'], ascending=[True, True, True]).head(5)

    print(f"\nTop positive correlations for {bird}:")
    print(top_high[['metric', 'pearson_corr', 'spearman_corr', 'lasso_coef']])
    print(f"\nTop negative correlations for {bird}:")
    print(bottom_low[['metric', 'pearson_corr', 'spearman_corr', 'lasso_coef']])