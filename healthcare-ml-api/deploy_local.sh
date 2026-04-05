#!/bin/bash
# deploy_local.sh
# ─────────────────────────────────────────────────────────────
# Deploy the Healthcare ML API to local minikube cluster.
# Runs: train → build → push → deploy → test
#
# Usage:
#   chmod +x deploy_local.sh
#   ./deploy_local.sh

set -e  # Exit on any error

echo "╔══════════════════════════════════════════════╗"
echo "║  Healthcare ML API — Local K8s Deployment    ║"
echo "╚══════════════════════════════════════════════╝"

# ── Step 1: Train model ───────────────────────────────────────
echo ""
echo ">>> STEP 1/5: Training ML model..."
python src/train_model.py
echo "✅ Model trained"

# ── Step 2: Run tests ─────────────────────────────────────────
echo ""
echo ">>> STEP 2/5: Running tests..."
pytest tests/ -v --tb=short
echo "✅ Tests passed"

# ── Step 3: Build Docker image ────────────────────────────────
echo ""
echo ">>> STEP 3/5: Building Docker image..."

# Point Docker to minikube's Docker daemon
eval $(minikube docker-env)

docker build -t healthcare-ml-api:latest .
echo "✅ Docker image built"

# ── Step 4: Deploy to Kubernetes ─────────────────────────────
echo ""
echo ">>> STEP 4/5: Deploying to Kubernetes..."
kubectl apply -f k8s/deployment.yaml
kubectl rollout status deployment/healthcare-ml-api --timeout=120s
echo "✅ Kubernetes deployment complete"

# ── Step 5: Test the deployment ───────────────────────────────
echo ""
echo ">>> STEP 5/5: Testing live deployment..."
MINIKUBE_IP=$(minikube ip)
SERVICE_URL="http://${MINIKUBE_IP}:30080"

# Wait for service to be ready
sleep 5

# Health check
HEALTH=$(curl -s "${SERVICE_URL}/health")
echo "Health: $HEALTH"

# Test prediction
PREDICTION=$(curl -s -X POST "${SERVICE_URL}/predict" \
  -H "Content-Type: application/json" \
  -d '{"state":"CA","specialty":"Cardiology","drug_type":"brand","total_claims":100,"total_beneficiaries":45}')
echo "Prediction: $PREDICTION"

echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║  DEPLOYMENT COMPLETE ✓                       ║"
echo "║                                              ║"
echo "║  API URL: ${SERVICE_URL}"
echo "║  Docs:    ${SERVICE_URL}/docs"
echo "║                                              ║"
echo "║  kubectl get pods                            ║"
echo "║  kubectl get services                        ║"
echo "╚══════════════════════════════════════════════╝"
