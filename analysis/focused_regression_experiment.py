#!/usr/bin/env python3
"""
Focused Regression Modeling Experiment for Price Drop Prediction

This script conducts a focused regression modeling experiment to predict the 
price drop within 10 seconds after a funding rate settlement event.

Features used (from 4-hour k-line leading up to the event):
- fundingRate
- price_return (computed as (close - open) / open)

Target (from high-frequency data in the 10s after the event):
- delta (max_drop_percentage)

The experiment compares multiple regression models and analyzes feature importance.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import glob
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import r2_score, mean_squared_error
import optuna
from optuna.samplers import TPESampler

# Try to import XGBoost, set flag if available
try:
    import xgboost as xgb
    XGBOOST_AVAILABLE = True
except ImportError:
    XGBOOST_AVAILABLE = False
    print("XGBoost not available. Only Linear Regression and Random Forest will be used.")

# --- Configuration ---
RANDOM_STATE = 42
TEST_SIZE = 0.2
VALIDATION_SIZE = 0.2  # Not used in this version, but kept for consistency
TIME_WINDOW_SECONDS = 10
OPTUNA_TRIALS = 100  # Number of optimization trials
OPTUNA_TIMEOUT = 300  # Timeout in seconds (5 minutes per model)

def get_10s_price_drop(timestamp, symbol, agg_trades_dir):
    """
    For a given settlement timestamp and symbol, find the corresponding high-frequency
    trading data and calculate the max price drop in the next 10 seconds.
    """
    symbol_dir = os.path.join(agg_trades_dir, symbol)
    if not os.path.exists(symbol_dir):
        return None

    # Find the corresponding trading data file
    trade_file = None
    for file in os.listdir(symbol_dir):
        if file.endswith('_window.csv') and str(timestamp) in file:
            trade_file = os.path.join(symbol_dir, file)
            break
    
    if not trade_file:
        return None

    try:
        df = pd.read_csv(trade_file)
        
        # Define the 10-second window
        window_start = timestamp
        window_end = timestamp + (TIME_WINDOW_SECONDS * 1000)
        
        # Filter trades within the window
        window_df = df[(df['transact_time'] >= window_start) & (df['transact_time'] <= window_end)]

        if len(window_df) <= 1:
            return None

        # Calculate max price drop
        max_price = window_df['price'].max()
        min_price = window_df['price'].min()
        
        if max_price == 0: 
            return None
        
        delta = (max_price - min_price) / max_price * 100
        return delta

    except Exception:
        return None

def load_and_prepare_data(volume_dir, agg_trades_dir):
    """
    Load, merge, and prepare data for the prediction task.
    - Loads 4h k-line data for features.
    - Calculates 10s price drop from high-frequency data for the target.
    """
    print("Loading and preparing data...")
    
    # 1. Load 4-hour k-line data for features
    print("Step 1: Loading 4-hour k-line data for features...")
    csv_files = glob.glob(os.path.join(volume_dir, "*_historical_4h_volume.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No trading volume CSV files found in {volume_dir}")
    
    all_data = []
    for f in csv_files:
        try:
            df = pd.read_csv(f)
            # Standardize timestamp column from 'fundingTime' to 'timestamp'
            if 'fundingTime' in df.columns:
                df.rename(columns={'fundingTime': 'timestamp'}, inplace=True)
            elif 'open_time' in df.columns: # Keep fallback for other potential names
                df.rename(columns={'open_time': 'timestamp'}, inplace=True)

            # Add symbol to dataframe
            symbol = os.path.basename(f).split('_')[0]
            df['symbol'] = symbol

            required_cols = ['timestamp', 'symbol', 'open', 'close', 'fundingRate']
            if all(col in df.columns for col in required_cols):
                all_data.append(df)
            else:
                print(f"Warning: Skipping file {f} as it lacks required columns.")
        except Exception as e:
            print(f"Error loading {f}: {e}")

    if not all_data:
        raise ValueError("No valid data could be loaded from CSV files.")

    feature_df = pd.concat(all_data, ignore_index=True)
    
    # 2. Preprocess feature data
    print("Step 2: Preprocessing feature data...")
    required_cols = ['timestamp', 'symbol', 'open', 'close', 'fundingRate']
    
    for col in ['timestamp', 'open', 'close', 'fundingRate']:
        feature_df[col] = pd.to_numeric(feature_df[col], errors='coerce')
    feature_df = feature_df.dropna(subset=required_cols)

    # Calculate price_return feature
    feature_df['price_return'] = (feature_df['close'] - feature_df['open']) / feature_df['open']
    
    # Filter out rows where funding rate is not negative
    feature_df = feature_df[feature_df['fundingRate'] < 0].copy()
    print(f"Found {len(feature_df)} events with negative funding rates.")

    # 3. Calculate target variable (10s price drop)
    print("Step 3: Calculating target variable (10s price drop) from aggTrades...")
    feature_df['delta'] = feature_df.apply(
        lambda row: get_10s_price_drop(int(row['timestamp']), row['symbol'], agg_trades_dir),
        axis=1
    )
    
    # 4. Finalize dataset
    print("Step 4: Finalizing dataset...")
    final_df = feature_df.dropna(subset=['fundingRate', 'price_return', 'delta']).copy()
    
    print(f"Successfully prepared {len(final_df)} samples for modeling.")
    
    if len(final_df) == 0:
        raise ValueError("No data available after processing. Check paths and data integrity.")
        
    return final_df

def prepare_features_and_target(df):
    """
    Prepare features and target variable for modeling.
    """
    print("Preparing features and target...")
    
    feature_columns = ['fundingRate', 'price_return']
    target_column = 'delta'
    
    X = df[feature_columns]
    y = df[target_column]
    
    print(f"Features: {feature_columns}")
    print(f"Target: {target_column}")
    print(f"Feature matrix shape: {X.shape}")
    print(f"Target vector shape: {y.shape}")
    
    return X, y

def optimize_randomforest_hyperparameters(X_train, y_train, X_val, y_val):
    """
    Optimize RandomForest hyperparameters using Optuna.
    """
    print("🔍 Optimizing RandomForest hyperparameters with Optuna...")
    
    def objective(trial):
        # Suggest hyperparameters
        n_estimators = trial.suggest_int('n_estimators', 50, 500, step=50)
        max_depth = trial.suggest_int('max_depth', 3, 20)
        min_samples_split = trial.suggest_int('min_samples_split', 2, 20)
        min_samples_leaf = trial.suggest_int('min_samples_leaf', 1, 10)
        max_features = trial.suggest_categorical('max_features', ['sqrt', 'log2', None])
        bootstrap = trial.suggest_categorical('bootstrap', [True, False])
        
        # Create and train model
        model = RandomForestRegressor(
            n_estimators=n_estimators,
            max_depth=max_depth,
            min_samples_split=min_samples_split,
            min_samples_leaf=min_samples_leaf,
            max_features=max_features,
            bootstrap=bootstrap,
            random_state=RANDOM_STATE,
            n_jobs=-1
        )
        
        model.fit(X_train, y_train)
        y_pred = model.predict(X_val)
        
        # Use negative MSE as objective (Optuna maximizes)
        mse = mean_squared_error(y_val, y_pred)
        return -mse
    
    # Create study and optimize
    study = optuna.create_study(
        direction='maximize',
        sampler=TPESampler(seed=RANDOM_STATE)
    )
    
    study.optimize(objective, n_trials=OPTUNA_TRIALS, timeout=OPTUNA_TIMEOUT, show_progress_bar=True)
    
    print(f"✅ RandomForest optimization completed!")
    print(f"   Best MSE: {-study.best_value:.6f}")
    print(f"   Best params: {study.best_params}")
    
    return study.best_params

def optimize_xgboost_hyperparameters(X_train, y_train, X_val, y_val, quantile_alpha=0.5):
    """
    Optimize XGBoost hyperparameters using Optuna for a specific quantile.
    """
    print(f"🔍 Optimizing XGBoost hyperparameters for quantile {quantile_alpha} with Optuna...")
    
    def objective(trial):
        # Suggest hyperparameters
        n_estimators = trial.suggest_int('n_estimators', 50, 500, step=50)
        max_depth = trial.suggest_int('max_depth', 3, 15)
        learning_rate = trial.suggest_float('learning_rate', 0.01, 0.3, log=True)
        subsample = trial.suggest_float('subsample', 0.6, 1.0)
        colsample_bytree = trial.suggest_float('colsample_bytree', 0.6, 1.0)
        reg_alpha = trial.suggest_float('reg_alpha', 0.0, 10.0)
        reg_lambda = trial.suggest_float('reg_lambda', 0.0, 10.0)
        
        # Create and train model
        model = xgb.XGBRegressor(
            objective='reg:quantileerror',
            quantile_alpha=quantile_alpha,
            n_estimators=n_estimators,
            max_depth=max_depth,
            learning_rate=learning_rate,
            subsample=subsample,
            colsample_bytree=colsample_bytree,
            reg_alpha=reg_alpha,
            reg_lambda=reg_lambda,
            random_state=RANDOM_STATE,
            n_jobs=-1,
            verbosity=0  # Suppress XGBoost output
        )
        
        model.fit(X_train, y_train)
        y_pred = model.predict(X_val)
        
        # Use negative MSE as objective (Optuna maximizes)
        mse = mean_squared_error(y_val, y_pred)
        return -mse
    
    # Create study and optimize
    study = optuna.create_study(
        direction='maximize',
        sampler=TPESampler(seed=RANDOM_STATE)
    )
    
    study.optimize(objective, n_trials=OPTUNA_TRIALS, timeout=OPTUNA_TIMEOUT, show_progress_bar=True)
    
    print(f"✅ XGBoost optimization for quantile {quantile_alpha} completed!")
    print(f"   Best MSE: {-study.best_value:.6f}")
    print(f"   Best params: {study.best_params}")
    
    return study.best_params

def train_and_evaluate_models_with_quantiles(X_train, X_test, y_train, y_test):
    """
    Trains and evaluates a set of quantile regression models for different model types.
    For Linear Regression, it approximates quantiles using residuals.
    """
    print("Training and evaluating quantile regression models for multiple types...")

    # --- Feature Scaling ---
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # --- Define Base Models and Quantile Alphas ---
    model_configs = {
        'Linear': {
            'model': LinearRegression(),
            'quantiles': [0.025, 0.5, 0.975] # These will be approximated
        },
        'RandomForest': {
            'model': RandomForestRegressor(random_state=RANDOM_STATE, n_jobs=-1),
            'quantiles': [0.025, 0.5, 0.975]
        },
    }
    if XGBOOST_AVAILABLE:
        model_configs['XGBoost'] = {
            'model': xgb.XGBRegressor(random_state=RANDOM_STATE, n_jobs=-1),
            'quantiles': [0.025, 0.5, 0.975]
        }

    trained_models = {}
    all_results_df = pd.DataFrame({
        'fundingRate': X_test['fundingRate'],
        'price_return': X_test['price_return'],
        'actual_delta': y_test
    })

    for model_name, config in model_configs.items():
        print(f"\n--- Training {model_name} Quantile Models ---")
        models_for_type = {}

        if model_name == 'Linear':
            # Train a standard Linear Regression model for the median
            median_model = config['model']
            median_model.fit(X_train_scaled, y_train)
            models_for_type['q0.5'] = median_model

            # Approximate quantiles using residuals
            y_train_pred_median = median_model.predict(X_train_scaled)
            residuals = y_train - y_train_pred_median
            
            # Calculate percentiles of residuals
            lower_bound_offset = np.percentile(residuals, 2.5)
            upper_bound_offset = np.percentile(residuals, 97.5)

            # Store dummy models that just apply the offset for prediction
            class QuantileLinearPredictor:
                def __init__(self, base_model, offset):
                    self.base_model = base_model
                    self.offset = offset
                def predict(self, X):
                    return self.base_model.predict(X) + self.offset
            
            models_for_type['q0.025'] = QuantileLinearPredictor(median_model, lower_bound_offset)
            models_for_type['q0.975'] = QuantileLinearPredictor(median_model, upper_bound_offset)

        elif model_name == 'RandomForest':
            # For RandomForest, we need to implement quantile estimation differently
            # since RandomForestRegressor doesn't support quantile loss directly
            print(f"  Training RandomForest model for quantile estimation...")
            
            # Optimize hyperparameters
            X_train_rf, X_val_rf, y_train_rf, y_val_rf = train_test_split(
                X_train_scaled, y_train, test_size=VALIDATION_SIZE, random_state=RANDOM_STATE
            )
            best_params = optimize_randomforest_hyperparameters(X_train_rf, y_train_rf, X_val_rf, y_val_rf)
            
            # Train a single RandomForest model
            base_model = RandomForestRegressor(
                random_state=RANDOM_STATE, n_jobs=-1, **best_params
            )
            base_model.fit(X_train_scaled, y_train)
            
            # For quantile estimation with RandomForest, we use the individual tree predictions
            # to estimate quantiles from the distribution of predictions
            def predict_quantiles(X, quantiles=[0.025, 0.5, 0.975]):
                """
                Predict quantiles using individual tree predictions from RandomForest
                """
                # Get predictions from all individual trees
                tree_predictions = np.array([tree.predict(X) for tree in base_model.estimators_])
                
                # Calculate quantiles across tree predictions for each sample
                quantile_predictions = {}
                for q in quantiles:
                    quantile_predictions[q] = np.percentile(tree_predictions, q * 100, axis=0)
                
                return quantile_predictions
            
            # Create quantile predictors using the tree ensemble
            class RandomForestQuantilePredictor:
                def __init__(self, base_model, quantile):
                    self.base_model = base_model
                    self.quantile = quantile
                
                def predict(self, X):
                    # Get predictions from all trees
                    tree_predictions = np.array([tree.predict(X) for tree in self.base_model.estimators_])
                    # Return the specified quantile across trees
                    return np.percentile(tree_predictions, self.quantile * 100, axis=0)
            
            # Create quantile predictors
            models_for_type['q0.025'] = RandomForestQuantilePredictor(base_model, 0.025)
            models_for_type['q0.5'] = RandomForestQuantilePredictor(base_model, 0.5)  # median
            models_for_type['q0.975'] = RandomForestQuantilePredictor(base_model, 0.975)

        elif model_name == 'XGBoost':
            # XGBoost supports quantile regression via 'reg:quantileloss' objective
            for q in config['quantiles']:
                print(f"  Training XGBoost model for quantile={q}...")
                
                # Optimize hyperparameters
                X_train_xgb, X_val_xgb, y_train_xgb, y_val_xgb = train_test_split(
                    X_train_scaled, y_train, test_size=VALIDATION_SIZE, random_state=RANDOM_STATE
                )
                best_params = optimize_xgboost_hyperparameters(X_train_xgb, y_train_xgb, X_val_xgb, y_val_xgb, quantile_alpha=q)
                
                model = xgb.XGBRegressor(
                    objective='reg:quantileerror',
                    quantile_alpha=q,
                    random_state=RANDOM_STATE,
                    n_jobs=-1,
                    **best_params
                )
                model.fit(X_train_scaled, y_train)
                models_for_type[f'q{q}'] = model

        trained_models[model_name] = models_for_type

        # --- Evaluate and Collect Results for this Model Type ---
        y_lower = models_for_type['q0.025'].predict(X_test_scaled)
        y_median = models_for_type['q0.5'].predict(X_test_scaled)
        y_upper = models_for_type['q0.975'].predict(X_test_scaled)

        # Ensure lower bound is always less than or equal to upper bound
        y_lower = np.minimum(y_lower, y_upper)
        y_upper = np.maximum(y_lower, y_upper)

        # Add predictions to the all_results_df
        all_results_df[f'{model_name}_pred_lower_bound'] = y_lower
        all_results_df[f'{model_name}_pred_median'] = y_median
        all_results_df[f'{model_name}_pred_upper_bound'] = y_upper

        # Calculate Coverage
        coverage = ((all_results_df['actual_delta'] >= all_results_df[f'{model_name}_pred_lower_bound']) & 
                    (all_results_df['actual_delta'] <= all_results_df[f'{model_name}_pred_upper_bound'])).mean()
        print(f"  {model_name} 95% Interval Coverage: {coverage:.2%}")
        
        # Calculate Median R-squared
        median_r2 = r2_score(all_results_df['actual_delta'], all_results_df[f'{model_name}_pred_median'])
        print(f"  {model_name} Median Prediction R²: {median_r2:.4f}")

    return all_results_df, trained_models, scaler

def plot_quantile_results(all_results_df, X_test, trained_models, scaler):
    """
    Plots the actual price drops, median predictions, and 95% confidence intervals
    against funding rate for different models on a single plot.
    """
    print("\n--- Generating Comparative Plots for 95% Confidence Intervals ---")

    # Create a single figure with subplots for better visualization
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # Main comparative plot - All models together
    ax1.scatter(all_results_df['fundingRate'], all_results_df['actual_delta'], 
               alpha=0.3, s=15, label='Actual Price Drops', color='gray')

    # Create a range of funding rates for plotting smooth prediction lines
    funding_rate_range = np.linspace(X_test['fundingRate'].min(), X_test['fundingRate'].max(), 100).reshape(-1, 1)
    price_return_mean = X_test['price_return'].mean()
    
    # Create feature matrix with proper feature names
    feature_names = ['fundingRate', 'price_return']
    X_plot_fr = np.hstack((funding_rate_range, np.full_like(funding_rate_range, price_return_mean)))
    
    # Create DataFrame with proper column names to avoid the warning
    X_plot_df = pd.DataFrame(X_plot_fr, columns=feature_names)
    X_plot_fr_scaled = scaler.transform(X_plot_df)

    colors = {'Linear': 'red', 'RandomForest': 'blue', 'XGBoost': 'green'}
    linestyles = {'Linear': '-', 'RandomForest': '--', 'XGBoost': ':'}

    model_performance = {}
    
    for model_name, models_for_type in trained_models.items():
        # Predict for plotting lines
        y_lower_plot_fr = models_for_type['q0.025'].predict(X_plot_fr_scaled)
        y_median_plot_fr = models_for_type['q0.5'].predict(X_plot_fr_scaled)
        y_upper_plot_fr = models_for_type['q0.975'].predict(X_plot_fr_scaled)

        # Ensure lower bound is always less than or equal to upper bound for plotting
        y_lower_plot_fr = np.minimum(y_lower_plot_fr, y_upper_plot_fr)
        y_upper_plot_fr = np.maximum(y_lower_plot_fr, y_upper_plot_fr)

        # Plot median prediction line
        ax1.plot(funding_rate_range, y_median_plot_fr, 
                color=colors[model_name], linestyle=linestyles[model_name], 
                linewidth=2.5, label=f'{model_name} Median')
        
        # Plot 95% Confidence Interval
        ax1.fill_between(funding_rate_range.flatten(), y_lower_plot_fr, y_upper_plot_fr, 
                        color=colors[model_name], alpha=0.15, 
                        label=f'{model_name} 95% CI')
        
        # Store performance metrics
        coverage = ((all_results_df['actual_delta'] >= all_results_df[f'{model_name}_pred_lower_bound']) & 
                   (all_results_df['actual_delta'] <= all_results_df[f'{model_name}_pred_upper_bound'])).mean()
        
        from sklearn.metrics import r2_score, mean_squared_error
        r2 = r2_score(all_results_df['actual_delta'], all_results_df[f'{model_name}_pred_median'])
        mse = mean_squared_error(all_results_df['actual_delta'], all_results_df[f'{model_name}_pred_median'])
        
        model_performance[model_name] = {'coverage': coverage, 'r2': r2, 'mse': mse}

    ax1.set_xlabel('Funding Rate (%)', fontsize=12)
    ax1.set_ylabel('Max Price Drop (%)', fontsize=12)
    ax1.set_title('Funding Rate vs. Max Price Drop\n(All Models Comparison)', fontsize=14, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    
    # Plot 2: Model Performance Comparison (R²)
    model_names = list(model_performance.keys())
    r2_scores = [model_performance[name]['r2'] for name in model_names]
    
    bars = ax2.bar(model_names, r2_scores, color=[colors[name] for name in model_names], alpha=0.7)
    ax2.set_xlabel('Models', fontsize=12)
    ax2.set_ylabel('R² Score', fontsize=12)
    ax2.set_title('Model Performance (R²)', fontsize=14, fontweight='bold')
    ax2.grid(True, alpha=0.3, axis='y')
    
    # Add value labels on bars
    for bar, score in zip(bars, r2_scores):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + 0.01,
                f'{score:.3f}', ha='center', va='bottom', fontsize=11, fontweight='bold')
    
    # Plot 3: Coverage Comparison
    coverage_scores = [model_performance[name]['coverage'] * 100 for name in model_names]
    
    bars = ax3.bar(model_names, coverage_scores, color=[colors[name] for name in model_names], alpha=0.7)
    ax3.set_xlabel('Models', fontsize=12)
    ax3.set_ylabel('95% Interval Coverage (%)', fontsize=12)
    ax3.set_title('95% Confidence Interval Coverage', fontsize=14, fontweight='bold')
    ax3.grid(True, alpha=0.3, axis='y')
    ax3.axhline(y=95, color='red', linestyle='--', alpha=0.8, label='Target (95%)')
    ax3.legend()
    
    # Add value labels on bars
    for bar, score in zip(bars, coverage_scores):
        height = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width()/2., height + 1,
                f'{score:.1f}%', ha='center', va='bottom', fontsize=11, fontweight='bold')
    
    # Plot 4: Prediction vs Actual for best model (highest R²)
    best_model_name = max(model_performance.keys(), key=lambda x: model_performance[x]['r2'])
    
    actual_values = all_results_df['actual_delta']
    predicted_values = all_results_df[f'{best_model_name}_pred_median']
    
    ax4.scatter(actual_values, predicted_values, alpha=0.6, s=30, 
               color=colors[best_model_name], label=f'{best_model_name} Predictions')
    
    # Add perfect prediction line
    min_val = min(actual_values.min(), predicted_values.min())
    max_val = max(actual_values.max(), predicted_values.max())
    ax4.plot([min_val, max_val], [min_val, max_val], 'k--', alpha=0.8, 
            linewidth=2, label='Perfect Prediction')
    
    ax4.set_xlabel('Actual Max Price Drop (%)', fontsize=12)
    ax4.set_ylabel('Predicted Max Price Drop (%)', fontsize=12)
    ax4.set_title(f'Prediction vs Actual\n({best_model_name}, R²={model_performance[best_model_name]["r2"]:.3f})', 
                 fontsize=14, fontweight='bold')
    ax4.grid(True, alpha=0.3)
    ax4.legend()
    
    plt.tight_layout()
    plt.savefig('comprehensive_regression_analysis.png', dpi=300, bbox_inches='tight')
    print("📊 Comprehensive regression analysis plot saved to: comprehensive_regression_analysis.png")
    
    # Print summary
    print(f"\n--- Model Performance Summary ---")
    print(f"{'Model':<15} | {'R²':<8} | {'Coverage':<10} | {'MSE':<10}")
    print("-" * 50)
    for name in model_names:
        perf = model_performance[name]
        print(f"{name:<15} | {perf['r2']:<8.3f} | {perf['coverage']*100:<10.1f} | {perf['mse']:<10.3f}")
    
    print(f"\n🏆 Best Model: {best_model_name} (R² = {model_performance[best_model_name]['r2']:.3f})")
    
    plt.close()

def main():
    """
    Main function to run the focused regression modeling experiment.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    volume_dir = os.path.join(script_dir, 'Trading Volume History')
    agg_trades_dir = os.path.join(script_dir, 'Negative Funding AggTrades')
    
    if not os.path.exists(volume_dir) or not os.path.exists(agg_trades_dir):
        print(f"Error: Required data directories not found.")
        print(f"Ensure 'Trading Volume History' and 'Negative Funding AggTrades' exist.")
        return
    
    try:
        # Step 1: Load and prepare data
        master_df = load_and_prepare_data(volume_dir, agg_trades_dir)
        
        # Step 2: Prepare features and target
        X, y = prepare_features_and_target(master_df)
        
        # Step 3: Split data (no validation set needed for this approach)
        print("Splitting data...")
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE
        )
        
        print(f"Training set: {len(X_train)} samples")
        print(f"Test set: {len(X_test)} samples")
        
        # Step 4: Train and evaluate quantile models
        all_results_df, trained_models, scaler = train_and_evaluate_models_with_quantiles(X_train, X_test, y_train, y_test)
        
        # Step 5: Plot results
        plot_quantile_results(all_results_df, X_test, trained_models, scaler)
        
    except Exception as e:
        print(f"An error occurred during the experiment: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()