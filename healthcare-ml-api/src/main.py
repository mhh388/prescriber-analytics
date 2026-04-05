"""
src/main.py
───────────
FastAPI application for drug cost prediction.

Endpoints:
  GET  /           — Health check
  GET  /health     — Detailed health + model info
  POST /predict    — Predict drug cost per claim
  POST /predict/batch — Batch predictions
  GET  /model/info — Model metadata

Run locally:
  uvicorn src.main:app --reload --port 8000

Then test:
  curl http://localhost:8000/health
  curl -X POST http://localhost:8000/predict \
    -H "Content-Type: application/json" \
    -d '{"state":"CA","specialty":"Cardiology","drug_type":"brand","total_claims":100,"total_beneficiaries":45}'
"""

import json
import joblib
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, field_validator

# ── Paths ─────────────────────────────────────────────────────
BASE_DIR      = Path(__file__).resolve().parent.parent
MODELS_DIR    = BASE_DIR / "models"
MODEL_PATH    = MODELS_DIR / "drug_cost_model.joblib"
METADATA_PATH = MODELS_DIR / "model_metadata.json"

# ── Load model at startup ─────────────────────────────────────
model    = None
metadata = None

def load_model():
    global model, metadata
    if MODEL_PATH.exists():
        model = joblib.load(MODEL_PATH)
        with open(METADATA_PATH) as f:
            metadata = json.load(f)
    else:
        raise RuntimeError(
            f"Model not found at {MODEL_PATH}. "
            f"Run: python src/train_model.py"
        )

# ── FastAPI app ───────────────────────────────────────────────
app = FastAPI(
    title="Healthcare Drug Cost Prediction API",
    description=(
        "Predicts Medicare drug cost per claim using CMS Part D data. "
        "Built with scikit-learn GradientBoosting, deployed on Kubernetes."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)


@app.on_event("startup")
async def startup_event():
    load_model()


# ── Request / Response schemas ────────────────────────────────

class PredictRequest(BaseModel):
    state: str = Field(
        ...,
        description="Prescriber US state abbreviation (e.g. 'CA', 'NY')",
        examples=["CA"]
    )
    specialty: str = Field(
        ...,
        description="Prescriber medical specialty (e.g. 'Cardiology')",
        examples=["Cardiology"]
    )
    drug_type: str = Field(
        ...,
        description="Drug type: 'brand' or 'generic'",
        examples=["brand"]
    )
    total_claims: float = Field(
        ...,
        description="Total number of claims",
        ge=1,
        examples=[100]
    )
    total_beneficiaries: float = Field(
        default=0,
        description="Total number of beneficiaries",
        ge=0,
        examples=[45]
    )
    drug_name: str = Field(
    default="Unknown",
    description="Generic drug name (e.g. 'Apixaban', 'Metformin Hcl')",
    examples=["Apixaban"]
    )

    @field_validator("drug_type")
    @classmethod
    def validate_drug_type(cls, v):
        if v.lower() not in ("brand", "generic"):
            raise ValueError("drug_type must be 'brand' or 'generic'")
        return v.lower()

    @field_validator("state")
    @classmethod
    def validate_state(cls, v):
        return v.upper().strip()


class PredictResponse(BaseModel):
    predicted_cost_per_claim: float
    prediction_lower_bound:   float
    prediction_upper_bound:   float
    confidence_note:          str
    input_features:           dict
    model_version:            str
    predicted_at:             str


class BatchPredictRequest(BaseModel):
    requests: list[PredictRequest]


class HealthResponse(BaseModel):
    status:        str
    model_loaded:  bool
    model_version: Optional[str]
    model_metrics: Optional[dict]
    uptime_note:   str


# ── Helper ────────────────────────────────────────────────────

def make_prediction(req: PredictRequest) -> float:
    """Run the model pipeline on a single request."""
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    input_df = pd.DataFrame([{
        "Prscrbr_State_Abrvtn": req.state,
        "Prscrbr_Type":         req.specialty,
        "drug_type":            req.drug_type,
        "Gnrc_Name":            req.drug_name,
        "Tot_Clms":             req.total_claims,
        "Tot_Benes":            req.total_beneficiaries,
    }])

    prediction = model.predict(input_df)[0]
    return max(0.0, round(float(prediction), 2))


# ── Endpoints ─────────────────────────────────────────────────

@app.get("/", tags=["Health"])
async def root():
    return {
        "service": "Healthcare Drug Cost Prediction API",
        "status":  "running",
        "docs":    "/docs",
    }


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health():
    return HealthResponse(
        status        = "healthy" if model is not None else "degraded",
        model_loaded  = model is not None,
        model_version = metadata.get("model_version") if metadata else None,
        model_metrics = metadata.get("metrics") if metadata else None,
        uptime_note   = f"Service running as of {datetime.now(timezone.utc).isoformat()}",
    )


@app.get("/model/info", tags=["Model"])
async def model_info():
    if metadata is None:
        raise HTTPException(status_code=503, detail="Model metadata not available")
    return metadata


@app.post("/predict", response_model=PredictResponse, tags=["Prediction"])
async def predict(req: PredictRequest):
    """
    Predict the expected drug cost per claim for a given
    prescriber specialty, state, drug type, and claim volume.
    """
    predicted = make_prediction(req)

    # Simple uncertainty bounds (±20% — in production use proper conformal prediction)
    lower = round(predicted * 0.80, 2)
    upper = round(predicted * 1.20, 2)

    return PredictResponse(
        predicted_cost_per_claim = predicted,
        prediction_lower_bound   = lower,
        prediction_upper_bound   = upper,
        confidence_note          = "Bounds represent ±20% empirical range. Use with clinical judgment.",
        input_features           = req.model_dump(),
        model_version            = metadata.get("model_version", "unknown") if metadata else "unknown",
        predicted_at             = datetime.now(timezone.utc).isoformat(),
    )


@app.post("/predict/batch", tags=["Prediction"])
async def predict_batch(batch: BatchPredictRequest):
    """Run predictions for multiple inputs in one call."""
    if len(batch.requests) > 100:
        raise HTTPException(
            status_code=400,
            detail="Batch size limited to 100 requests"
        )

    results = []
    for i, req in enumerate(batch.requests):
        try:
            predicted = make_prediction(req)
            results.append({
                "index":                   i,
                "status":                  "success",
                "predicted_cost_per_claim": predicted,
                "input":                   req.model_dump(),
            })
        except Exception as e:
            results.append({
                "index":  i,
                "status": "error",
                "error":  str(e),
                "input":  req.model_dump(),
            })

    return {
        "total":     len(batch.requests),
        "succeeded": sum(1 for r in results if r["status"] == "success"),
        "failed":    sum(1 for r in results if r["status"] == "error"),
        "results":   results,
    }
