"""
Hugging Face Models API ingester.

Fetches model metadata sorted by downloads — gives us the most
impactful models in the AI ecosystem.

API docs: https://huggingface.co/docs/hub/api#get-apimodels
"""

import os
from loguru import logger
from ingestion.base import BaseIngester

# Pipeline types map to AI task categories
# Fetching specific types gives us structured, comparable data
PIPELINE_TYPES = [
    "text-generation",
    "text-classification",
    "token-classification",
    "question-answering",
    "translation",
    "summarization",
    "image-classification",
    "object-detection",
    "automatic-speech-recognition",
    "text-to-image",
]

MODELS_PER_TYPE = 50   # 50 models × 10 types = 500 models total


class HuggingFaceIngester(BaseIngester):

    def __init__(self, raw_base_path: str):
        super().__init__("huggingface", raw_base_path)
        token = os.getenv("HUGGINGFACE_TOKEN")
        self.headers = {"Accept": "application/json"}
        if token:
            self.headers["Authorization"] = f"Bearer {token}"

    def fetch(self) -> list[dict]:
        all_models = []
        seen_ids = set()

        for pipeline_type in PIPELINE_TYPES:
            logger.info(f"Fetching HuggingFace models: pipeline={pipeline_type}")
            try:
                models = self._get(
                    "https://huggingface.co/api/models",
                    params={
                        "pipeline_tag": pipeline_type,
                        "sort": "downloads",
                        "direction": -1,       # descending
                        "limit": MODELS_PER_TYPE,
                        "full": "true",        # include tags, card data
                    },
                    headers=self.headers,
                )

                for model in models:
                    model_id = model.get("modelId") or model.get("id")
                    if model_id in seen_ids:
                        continue
                    seen_ids.add(model_id)
                    all_models.append(self._extract_fields(model, pipeline_type))

            except Exception as e:
                logger.error(f"Failed pipeline_type='{pipeline_type}': {e}")

        logger.info(f"HuggingFace total unique models fetched: {len(all_models)}")
        return all_models

    def _extract_fields(self, model: dict, pipeline_type: str) -> dict:
        """
        Normalize HuggingFace model metadata.

        HuggingFace returns inconsistent field names across model types
        (some use 'modelId', others use 'id'). We normalize here so
        the Silver layer Spark job sees a consistent schema.
        """
        model_id = model.get("modelId") or model.get("id", "")
        # author is the org/user prefix before the slash: "openai/gpt-4" → "openai"
        author = model_id.split("/")[0] if "/" in model_id else model_id

        return {
            "model_id": model_id,
            "author": author,
            "model_name": model_id.split("/")[-1] if "/" in model_id else model_id,
            "pipeline_tag": pipeline_type,
            "downloads": model.get("downloads", 0),
            "likes": model.get("likes", 0),
            "tags": model.get("tags", []),
            "library_name": model.get("library_name"),
            "created_at": model.get("createdAt"),
            "last_modified": model.get("lastModified"),
            "private": model.get("private", False),
            "gated": model.get("gated", False),
        }
