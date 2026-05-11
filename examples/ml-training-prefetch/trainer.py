"""
ML Trainer — trains a user-activity classifier on prefetched feature records.

Task
----
Predict whether a user is "high-activity" in a given 30-second window based
on their event_count.  Labels:
  0 = low    (event_count ≤ 33rd percentile)
  1 = medium (33rd < event_count ≤ 66th percentile)
  2 = high   (event_count > 66th percentile)

Model
-----
Logistic Regression (one-vs-rest) implemented in pure Python + optional numpy.
When scikit-learn is installed a RandomForestClassifier is used instead for
a more realistic training workload.  Both paths consume identical FeatureRecord
inputs and produce identical TrainingResult outputs.

Model persistence
-----------------
The trained model is serialised with pickle and optionally uploaded to MinIO
at: models/streamforge/ml/<job_id>/model.pkl
"""

from __future__ import annotations

import json
import math
import os
import pickle
import random
import time
from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from feature_store import FeatureRecord


# ---------------------------------------------------------------------------
# Dataset preparation
# ---------------------------------------------------------------------------

@dataclass
class TrainingDataset:
    features: List[List[float]]   # [[event_count, event_rate, user_id_norm], ...]
    labels: List[int]             # 0 / 1 / 2
    user_ids: List[int]
    n_classes: int = 3

    @property
    def n_samples(self) -> int:
        return len(self.labels)

    @property
    def class_counts(self) -> Dict[int, int]:
        counts: Dict[int, int] = {0: 0, 1: 0, 2: 0}
        for lbl in self.labels:
            counts[lbl] = counts.get(lbl, 0) + 1
        return counts


def build_dataset(records: List[FeatureRecord]) -> TrainingDataset:
    """
    Convert FeatureRecords to a labelled feature matrix.

    Features per sample:
      x0 = log1p(event_count)           — main signal
      x1 = event_rate (events / window_s) — derived
      x2 = user_id / 200.0              — normalised user identity

    Labels are determined by the 33rd and 66th percentile of event_count
    across the dataset so they balance across any distribution.
    """
    if not records:
        raise ValueError("Cannot build a dataset from zero feature records.")

    counts = sorted(r.event_count for r in records)
    n = len(counts)
    p33 = counts[n // 3]
    p66 = counts[(2 * n) // 3]

    def label(ec: int) -> int:
        if ec <= p33:
            return 0
        if ec <= p66:
            return 1
        return 2

    features, labels, user_ids = [], [], []
    for r in records:
        features.append([
            math.log1p(r.event_count),
            r.event_rate,
            r.user_id / 200.0,
        ])
        labels.append(label(r.event_count))
        user_ids.append(r.user_id)

    return TrainingDataset(features=features, labels=labels, user_ids=user_ids)


def train_test_split(
    dataset: TrainingDataset,
    test_ratio: float = 0.2,
    seed: int = 42,
) -> Tuple[TrainingDataset, TrainingDataset]:
    rng = random.Random(seed)
    indices = list(range(dataset.n_samples))
    rng.shuffle(indices)
    split = int(len(indices) * (1 - test_ratio))
    train_idx, test_idx = indices[:split], indices[split:]

    def subset(idx: List[int]) -> TrainingDataset:
        return TrainingDataset(
            features=[dataset.features[i] for i in idx],
            labels=[dataset.labels[i] for i in idx],
            user_ids=[dataset.user_ids[i] for i in idx],
        )

    return subset(train_idx), subset(test_idx)


# ---------------------------------------------------------------------------
# Pure-Python logistic regression (stdlib-only fallback)
# ---------------------------------------------------------------------------

class _PurePythonLR:
    """
    Mini one-vs-rest logistic regression using gradient descent.
    No numpy required — uses only Python lists and math.
    """

    def __init__(self, n_classes: int = 3, lr: float = 0.05,
                 epochs: int = 40, seed: int = 42):
        self.n_classes = n_classes
        self.lr = lr
        self.epochs = epochs
        self._rng = random.Random(seed)
        self.weights: List[List[float]] = []
        self.biases: List[float] = []

    def _sigmoid(self, z: float) -> float:
        return 1.0 / (1.0 + math.exp(-max(-500.0, min(500.0, z))))

    def _dot(self, w: List[float], x: List[float]) -> float:
        return sum(wi * xi for wi, xi in zip(w, x))

    def fit(self, X: List[List[float]], y: List[int]) -> None:
        n_features = len(X[0])
        self.weights = [
            [self._rng.gauss(0, 0.01) for _ in range(n_features)]
            for _ in range(self.n_classes)
        ]
        self.biases = [0.0] * self.n_classes

        for _ in range(self.epochs):
            for xi, yi in zip(X, y):
                for c in range(self.n_classes):
                    target = 1.0 if yi == c else 0.0
                    pred = self._sigmoid(self._dot(self.weights[c], xi) + self.biases[c])
                    err = pred - target
                    self.biases[c] -= self.lr * err
                    for j in range(n_features):
                        self.weights[c][j] -= self.lr * err * xi[j]

    def predict(self, X: List[List[float]]) -> List[int]:
        preds = []
        for xi in X:
            scores = [
                self._sigmoid(self._dot(self.weights[c], xi) + self.biases[c])
                for c in range(self.n_classes)
            ]
            preds.append(scores.index(max(scores)))
        return preds

    def score(self, X: List[List[float]], y: List[int]) -> float:
        preds = self.predict(X)
        return sum(p == t for p, t in zip(preds, y)) / len(y)


# ---------------------------------------------------------------------------
# scikit-learn wrapper (used when available)
# ---------------------------------------------------------------------------

def _sklearn_model():
    try:
        from sklearn.ensemble import RandomForestClassifier  # type: ignore
        return RandomForestClassifier(n_estimators=50, max_depth=6, random_state=42)
    except ImportError:
        return None


# ---------------------------------------------------------------------------
# Training result
# ---------------------------------------------------------------------------

@dataclass
class TrainingResult:
    job_id: str
    n_train: int
    n_test: int
    accuracy: float
    train_duration_s: float
    backend: str          # "sklearn" | "pure-python"
    model_path: Optional[str] = None     # local path to saved model
    minio_model_key: Optional[str] = None  # MinIO object key if uploaded


# ---------------------------------------------------------------------------
# Main trainer entry point
# ---------------------------------------------------------------------------

def train(
    records: List[FeatureRecord],
    job_id: str = "ml-training",
    model_dir: Optional[Path] = None,
) -> TrainingResult:
    """
    Build a dataset from feature records, train a classifier, evaluate it,
    and persist the model artifact.
    """
    print(f"[TRAINER] Building dataset from {len(records):,} feature records …")
    dataset = build_dataset(records)
    train_ds, test_ds = train_test_split(dataset)
    print(f"[TRAINER] train={train_ds.n_samples:,}  test={test_ds.n_samples:,}  "
          f"classes={dataset.class_counts}")

    sklearn_model = _sklearn_model()
    backend = "sklearn" if sklearn_model is not None else "pure-python"
    print(f"[TRAINER] Using backend: {backend}")

    t0 = time.perf_counter()
    if sklearn_model is not None:
        sklearn_model.fit(train_ds.features, train_ds.labels)
        accuracy = float(sklearn_model.score(test_ds.features, test_ds.labels))
        model_obj = sklearn_model
    else:
        lr = _PurePythonLR(n_classes=dataset.n_classes)
        lr.fit(train_ds.features, train_ds.labels)
        accuracy = lr.score(test_ds.features, test_ds.labels)
        model_obj = lr
    train_duration_s = time.perf_counter() - t0

    print(f"[TRAINER] Accuracy={accuracy:.4f}  train_time={train_duration_s:.2f}s")

    # Persist model
    model_path = _save_model(model_obj, job_id, model_dir)
    minio_key = _upload_model(model_path, job_id)

    return TrainingResult(
        job_id=job_id,
        n_train=train_ds.n_samples,
        n_test=test_ds.n_samples,
        accuracy=accuracy,
        train_duration_s=train_duration_s,
        backend=backend,
        model_path=str(model_path),
        minio_model_key=minio_key,
    )


# ---------------------------------------------------------------------------
# Model persistence helpers
# ---------------------------------------------------------------------------

def _save_model(model_obj: object, job_id: str, model_dir: Optional[Path]) -> Path:
    if model_dir is None:
        base = Path(os.environ.get("STREAMFORGE_DEMO_DIR", "/tmp/streamforge-demo"))
        model_dir = base / "models"
    model_dir.mkdir(parents=True, exist_ok=True)
    path = model_dir / f"{job_id}_model.pkl"
    path.write_bytes(pickle.dumps(model_obj))
    print(f"[TRAINER] Model saved to {path}")
    return path


def _upload_model(model_path: Path, job_id: str) -> Optional[str]:
    endpoint   = os.environ.get("MINIO_ENDPOINT",   "")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    bucket     = os.environ.get("MINIO_BUCKET",     "processed")
    secure     = os.environ.get("MINIO_SECURE",     "false").lower() == "true"

    if not endpoint:
        print("[TRAINER] MinIO not configured — model upload skipped.")
        return None

    try:
        from minio import Minio  # type: ignore
        client = Minio(endpoint, access_key=access_key,
                       secret_key=secret_key, secure=secure)
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
        key = f"streamforge/models/{job_id}/model.pkl"
        data = model_path.read_bytes()
        client.put_object(bucket, key, BytesIO(data), len(data),
                          content_type="application/octet-stream")
        print(f"[TRAINER] Model uploaded → minio://{bucket}/{key}")
        return key
    except Exception as exc:
        print(f"[TRAINER] Model upload failed (non-fatal): {exc}")
        return None
