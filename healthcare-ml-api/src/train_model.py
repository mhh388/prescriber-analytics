"""
src/train_model.py
──────────────────
Train a drug cost prediction model on CMS Medicare data.

What this does:
  1. Loads the processed CMS data from Project 1
  2. Engineers features (specialty, state, drug type, claims)
  3. Trains a GradientBoosting model to predict cost_per_claim
  4. Evaluates with cross-validation
  5. Saves model + feature encoder to models/

Run:
  python src/train_model.py
"""

import sys
import json
import joblib
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import cross_val_score, train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OrdinalEncoder
from loguru import logger

# ── Paths ─────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).resolve().parent.parent
MODELS_DIR  = BASE_DIR / "models"
DATA_FILE   = Path.home() / "Desktop/healthcare-azure-pipeline/data/raw/cms_partd_2023_sample.csv"
MODELS_DIR.mkdir(exist_ok=True)


# ── Feature Engineering ───────────────────────────────────────

CATEGORICAL_FEATURES = [
    "Prscrbr_State_Abrvtn",
    "Prscrbr_Type",
    "drug_type",
    "Gnrc_Name",
]

NUMERICAL_FEATURES = [
    "Tot_Clms",
    "Tot_Benes",
]

TARGET = "cost_per_claim"


def load_and_prepare_data(filepath: Path) -> pd.DataFrame:
    """Load CMS data and engineer features."""
    logger.info(f"Loading data from: {filepath.name}")
    df = pd.read_csv(filepath, low_memory=False)

    # Cast numeric columns
    for col in ["Tot_Clms", "Tot_Drug_Cst", "Tot_Benes"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Filter valid rows
    df = df[df["Tot_Clms"] > 0].dropna(subset=["Tot_Clms", "Tot_Drug_Cst"])

    # Engineer features
    df["cost_per_claim"] = df["Tot_Drug_Cst"] / df["Tot_Clms"]
    df["drug_type"] = df["Brnd_Name"].apply(
        lambda x: "brand" if pd.notna(x) and str(x).strip() != "" else "generic"
    )
    df["Gnrc_Name"]=df["Gnrc_Name"].fillna("Unknown")

    # Fill missing categoricals
    df["Prscrbr_Type"]         = df["Prscrbr_Type"].fillna("Unknown")
    df["Prscrbr_State_Abrvtn"] = df["Prscrbr_State_Abrvtn"].fillna("Unknown")
    df["Tot_Benes"]            = df["Tot_Benes"].fillna(0)

    # Remove extreme outliers (>99.5th percentile) for better model fit
    upper = df["cost_per_claim"].quantile(0.995)
    df    = df[df["cost_per_claim"] <= upper]

    logger.info(f"Prepared {len(df):,} rows for training")
    logger.info(f"Target range: ${df[TARGET].min():.2f} — ${df[TARGET].max():.2f}")
    logger.info(f"Target mean:  ${df[TARGET].mean():.2f}")

    return df


def build_pipeline() -> Pipeline:
    """Build sklearn pipeline with preprocessing + model."""
    preprocessor = ColumnTransformer(transformers=[
        ("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1),
         CATEGORICAL_FEATURES),
        ("num", "passthrough", NUMERICAL_FEATURES),
    ])

    model = GradientBoostingRegressor(
        n_estimators=200,
        max_depth=4,
        learning_rate=0.1,
        subsample=0.8,
        random_state=42,
    )

    return Pipeline([
        ("preprocessor", preprocessor),
        ("model", model),
    ])


def train_and_evaluate(df: pd.DataFrame) -> dict:
    """Train model and return evaluation metrics."""
    X = df[CATEGORICAL_FEATURES + NUMERICAL_FEATURES]
    y = df[TARGET]

    # Train/test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    logger.info(f"Training on {len(X_train):,} rows, testing on {len(X_test):,} rows")

    # Build and train
    pipeline = build_pipeline()
    pipeline.fit(X_train, y_train)

    # Evaluate
    y_pred = pipeline.predict(X_test)
    mae    = mean_absolute_error(y_test, y_pred)
    r2     = r2_score(y_test, y_pred)

    logger.info(f"Test MAE : ${mae:.2f}")
    logger.info(f"Test R²  : {r2:.4f}")

    # Cross-validation
    cv_scores = cross_val_score(pipeline, X_train, y_train, cv=5, scoring="r2")
    logger.info(f"CV R² (5-fold): {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")

    # Feature importance
    model       = pipeline.named_steps["model"]
    feature_names = CATEGORICAL_FEATURES + NUMERICAL_FEATURES
    importances = dict(zip(feature_names, model.feature_importances_.tolist()))
    importances = dict(sorted(importances.items(), key=lambda x: x[1], reverse=True))

    logger.info("Feature importances:")
    for feat, imp in importances.items():
        logger.info(f"  {feat:<35} {imp:.4f}")

    return {
        "mae":              round(mae, 2),
        "r2":               round(r2, 4),
        "cv_r2_mean":       round(cv_scores.mean(), 4),
        "cv_r2_std":        round(cv_scores.std(), 4),
        "train_size":       len(X_train),
        "test_size":        len(X_test),
        "feature_importance": importances,
        "pipeline":         pipeline,
    }


def save_model(pipeline: Pipeline, metrics: dict):
    """Save trained model and metadata."""
    model_path    = MODELS_DIR / "drug_cost_model.joblib"
    metadata_path = MODELS_DIR / "model_metadata.json"

    joblib.dump(pipeline, model_path)
    logger.success(f"Model saved: {model_path}")

    metadata = {
        "model_type":        "GradientBoostingRegressor",
        "target":            TARGET,
        "features":          CATEGORICAL_FEATURES + NUMERICAL_FEATURES,
        "categorical_features": CATEGORICAL_FEATURES,
        "numerical_features":   NUMERICAL_FEATURES,
        "metrics":           {k: v for k, v in metrics.items() if k != "pipeline"},
        "trained_at":        datetime.now(timezone.utc).isoformat(),
        "data_source":       "CMS Medicare Part D 2023",
        "model_version":     "1.0.0",
    }

    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)
    logger.success(f"Metadata saved: {metadata_path}")

    return model_path, metadata_path


# ── Main ──────────────────────────────────────────────────────

def run_training():
    logger.info("=" * 60)
    logger.info("  TRAINING — Drug Cost Prediction Model")
    logger.info("=" * 60)

    df      = load_and_prepare_data(DATA_FILE)
    results = train_and_evaluate(df)
    save_model(results["pipeline"], results)

    logger.info("=" * 60)
    logger.success("  TRAINING COMPLETE ✓")
    logger.info(f"  MAE  : ${results['mae']:.2f} per claim")
    logger.info(f"  R²   : {results['r2']}")
    logger.info(f"  CV R²: {results['cv_r2_mean']} ± {results['cv_r2_std']}")
    logger.info("=" * 60)

    return results


if __name__ == "__main__":
    from loguru import logger
    logger.remove()
    logger.add(sys.stderr, level="INFO", colorize=True)
    run_training()
