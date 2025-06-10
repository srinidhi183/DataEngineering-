#!/bin/bash

# === CONFIGURABLE ===
PROJECT_ID="crypto-data-pipeline-462115"
REGION="europe-west1"
IMAGE_NAME="crypto-price-job"

# === ASK FOR VERSION TAG ===
read -p "📝 Enter version tag (e.g., v2, fixed, rolling-avg): " TAG

# === FINAL IMAGE URI ===
IMAGE_URI="gcr.io/$PROJECT_ID/$IMAGE_NAME:$TAG"

echo "📦 Building Docker image: $IMAGE_URI"
gcloud builds submit --tag "$IMAGE_URI"

echo "🚀 Deploying Cloud Run Job: $IMAGE_NAME"
gcloud run jobs deploy "$IMAGE_NAME" \
  --image "$IMAGE_URI" \
  --region "$REGION" \
  --project "$PROJECT_ID"

echo "▶️ Executing Cloud Run Job: $IMAGE_NAME"
gcloud run jobs execute "$IMAGE_NAME" --region "$REGION"

echo "✅ Success! Deployed and executed image: $IMAGE_URI"
