"""
tests/test_api.py
──────────────────
Unit and integration tests for the Healthcare ML API.
Uses pytest + httpx TestClient — no real server needed.

Run:
  pytest tests/ -v
"""

import json
import joblib
import pytest
import numpy as np
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

# ── Mock model before importing app ──────────────────────────

class MockModel:
    """Fake sklearn pipeline for testing without real model file."""
    def predict(self, X):
        # Return realistic predictions based on drug_type
        results = []
        for _, row in X.iterrows():
            base = 500.0 if row.get("drug_type") == "brand" else 50.0
            results.append(base + row.get("Tot_Clms", 0) * 0.1)
        return np.array(results)


MOCK_METADATA = {
    "model_version": "1.0.0-test",
    "model_type":    "GradientBoostingRegressor",
    "target":        "cost_per_claim",
    "features":      ["Prscrbr_State_Abrvtn", "Prscrbr_Type", "drug_type", "Tot_Clms", "Tot_Benes"],
    "metrics": {
        "mae":        45.23,
        "r2":         0.72,
        "cv_r2_mean": 0.70,
    },
    "trained_at":    "2026-01-01T00:00:00Z",
    "data_source":   "CMS Medicare Part D 2023",
}


@pytest.fixture(autouse=True)
def mock_model_loading():
    """Auto-mock model loading for all tests."""
    with patch("src.main.model", MockModel()), \
         patch("src.main.metadata", MOCK_METADATA):
        yield


@pytest.fixture
def client():
    from src.main import app
    return TestClient(app)


# ── Health endpoint tests ─────────────────────────────────────

def test_root_returns_200(client):
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "running"
    assert "docs" in data


def test_health_returns_healthy(client):
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["model_loaded"] is True
    assert data["model_version"] == "1.0.0-test"


def test_model_info_returns_metadata(client):
    response = client.get("/model/info")
    assert response.status_code == 200
    data = response.json()
    assert "model_version" in data
    assert "metrics" in data
    assert data["model_type"] == "GradientBoostingRegressor"


# ── Predict endpoint tests ────────────────────────────────────

def test_predict_brand_drug(client):
    response = client.post("/predict", json={
        "state":               "CA",
        "specialty":           "Cardiology",
        "drug_type":           "brand",
        "total_claims":        100,
        "total_beneficiaries": 45,
    })
    assert response.status_code == 200
    data = response.json()
    assert "predicted_cost_per_claim" in data
    assert data["predicted_cost_per_claim"] > 0
    assert "prediction_lower_bound" in data
    assert "prediction_upper_bound" in data
    assert data["prediction_lower_bound"] < data["predicted_cost_per_claim"]
    assert data["prediction_upper_bound"] > data["predicted_cost_per_claim"]


def test_predict_generic_drug(client):
    response = client.post("/predict", json={
        "state":               "NY",
        "specialty":           "Family Practice",
        "drug_type":           "generic",
        "total_claims":        200,
        "total_beneficiaries": 80,
    })
    assert response.status_code == 200
    data = response.json()
    assert data["predicted_cost_per_claim"] > 0


def test_predict_state_uppercased(client):
    """State should be auto-uppercased."""
    response = client.post("/predict", json={
        "state":        "ca",     # lowercase
        "specialty":    "Cardiology",
        "drug_type":    "brand",
        "total_claims": 100,
    })
    assert response.status_code == 200
    data = response.json()
    assert data["input_features"]["state"] == "CA"


def test_predict_invalid_drug_type(client):
    """Invalid drug_type should return 422."""
    response = client.post("/predict", json={
        "state":        "CA",
        "specialty":    "Cardiology",
        "drug_type":    "premium",    # invalid
        "total_claims": 100,
    })
    assert response.status_code == 422


def test_predict_zero_claims_rejected(client):
    """Zero claims should be rejected (ge=1 constraint)."""
    response = client.post("/predict", json={
        "state":        "CA",
        "specialty":    "Cardiology",
        "drug_type":    "brand",
        "total_claims": 0,           # invalid
    })
    assert response.status_code == 422


def test_predict_missing_required_fields(client):
    """Missing required fields should return 422."""
    response = client.post("/predict", json={
        "state": "CA",
        # missing specialty, drug_type, total_claims
    })
    assert response.status_code == 422


def test_predict_returns_model_version(client):
    response = client.post("/predict", json={
        "state":        "TX",
        "specialty":    "Internal Medicine",
        "drug_type":    "generic",
        "total_claims": 500,
    })
    assert response.status_code == 200
    data = response.json()
    assert data["model_version"] == "1.0.0-test"
    assert "predicted_at" in data


# ── Batch predict tests ───────────────────────────────────────

def test_batch_predict(client):
    response = client.post("/predict/batch", json={
        "requests": [
            {"state": "CA", "specialty": "Cardiology",
             "drug_type": "brand",   "total_claims": 100},
            {"state": "NY", "specialty": "Family Practice",
             "drug_type": "generic", "total_claims": 200},
            {"state": "TX", "specialty": "Endocrinology",
             "drug_type": "brand",   "total_claims": 50},
        ]
    })
    assert response.status_code == 200
    data = response.json()
    assert data["total"]     == 3
    assert data["succeeded"] == 3
    assert data["failed"]    == 0
    assert len(data["results"]) == 3


def test_batch_predict_too_large(client):
    """Batch > 100 should be rejected."""
    requests = [
        {"state": "CA", "specialty": "Cardiology",
         "drug_type": "brand", "total_claims": 100}
    ] * 101

    response = client.post("/predict/batch", json={"requests": requests})
    assert response.status_code == 400
