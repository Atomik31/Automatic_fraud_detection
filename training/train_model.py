"""
Bloc 3 — Automatic Fraud Detection
Script d'entraînement du modèle de détection de fraude.

Usage :
    python train_model.py

Pré-requis :
    - fraudTest.csv dans le même dossier
    - .env avec MLFLOW_TRACKING_URI et EXPERIMENT_NAME
"""

import sys
import io
import os
import pandas as pd
import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature
from mlflow.tracking import MlflowClient

# Forcer UTF-8 sur Windows (emojis MLflow)
if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

from dotenv import load_dotenv
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, TargetEncoder
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score
from sklearn.base import BaseEstimator, TransformerMixin

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
DATA_PATH  = os.path.join(os.path.dirname(__file__), "fraudTest.csv")
TARGET     = "is_fraud"
TEST_SIZE  = 0.25
MODEL_NAME = "fraud_detector"
ALIAS      = "production"

mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "https://atomik31-mlflow.hf.space"))

# Créer l'experiment avec artifact location sur S3 si pas encore existant
experiment_name    = os.getenv("EXPERIMENT_NAME", "fraud-detection-v2")
artifact_location  = os.getenv("ARTIFACT_LOCATION", "s3://mlflow-lead")
client_tmp = MlflowClient()
exp = client_tmp.get_experiment_by_name(experiment_name)
if exp is None:
    client_tmp.create_experiment(experiment_name, artifact_location=artifact_location)
mlflow.set_experiment(experiment_name)

KEPT_COLS = [
    "cc_num", "category", "amt", "gender", "city", "state",
    "zip", "lat", "long", "city_pop", "job", "dob",
    "trans_num", "merch_lat", "merch_long",
    "trans_date_trans_time", TARGET,
]
NUM_COLS = [
    "cc_num", "amt", "zip", "lat", "long",
    "city_pop", "merch_lat", "merch_long",
    "Year", "Month", "day", "day_in_week",
]
CAT_COLS = ["category", "gender", "city", "state", "job", "dob", "trans_num"]


# ── Feature engineering ───────────────────────────────────────────────────────
class FraudFeatureEngineer(BaseEstimator, TransformerMixin):
    """Extrait les features temporelles et nettoie les colonnes inutiles."""

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        X = X.copy()
        time_col = "trans_date_trans_time" if "trans_date_trans_time" in X.columns else "current_time"
        dt = pd.to_datetime(X[time_col], dayfirst=False)
        X["Year"]        = dt.dt.year
        X["Month"]       = dt.dt.month
        X["day"]         = dt.dt.day
        X["day_in_week"] = dt.dt.dayofweek
        to_drop = [
            "trans_date_trans_time", "current_time",
            "street", "first", "last", "merchant",
            "is_fraud", "unix_time",
        ]
        return X.drop(columns=to_drop, errors="ignore")


# ── Données ───────────────────────────────────────────────────────────────────
print("Chargement du dataset...")
df = pd.read_csv(DATA_PATH, index_col=0)
df = df[[c for c in KEPT_COLS if c in df.columns]].copy()

X = df.drop(columns=[TARGET])
y = df[TARGET]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=TEST_SIZE, random_state=42, stratify=y
)
print(f"Train : {len(X_train):,} | Test : {len(X_test):,} | Fraude : {y.mean():.4%}")

# ── Pipeline ──────────────────────────────────────────────────────────────────
pipeline = Pipeline([
    ("feature_eng",  FraudFeatureEngineer()),
    ("preprocessor", ColumnTransformer([
        ("num", StandardScaler(), NUM_COLS),
        ("cat", TargetEncoder(),  CAT_COLS),
    ])),
    ("model", LogisticRegression(class_weight="balanced", max_iter=500, random_state=42)),
])

# ── Entraînement + MLflow ─────────────────────────────────────────────────────
print("Entraînement + tracking MLflow...")

with mlflow.start_run(run_name="LogReg_balanced") as run:
    pipeline.fit(X_train, y_train)
    y_pred = pipeline.predict(X_test)

    metrics = {
        "accuracy":  accuracy_score(y_test, y_pred),
        "f1_score":  f1_score(y_test, y_pred),
        "precision": precision_score(y_test, y_pred),
        "recall":    recall_score(y_test, y_pred),
    }
    mlflow.log_metrics(metrics)
    mlflow.log_params({
        "model": "LogisticRegression",
        "scaler": "StandardScaler",
        "encoder": "TargetEncoder",
        "class_weight": "balanced",
        "test_size": TEST_SIZE,
    })

    signature     = infer_signature(X_train, pipeline.predict(X_train))
    input_example = X_train.iloc[:3]

    mlflow.sklearn.log_model(
        pipeline,
        name=MODEL_NAME,
        registered_model_name=MODEL_NAME,
        signature=signature,
        input_example=input_example,
    )

    print(f"  Accuracy  : {metrics['accuracy']:.4f}")
    print(f"  F1        : {metrics['f1_score']:.4f}")
    print(f"  Precision : {metrics['precision']:.4f}")
    print(f"  Recall    : {metrics['recall']:.4f}")

# ── Alias @production ─────────────────────────────────────────────────────────
client   = MlflowClient()
versions = client.get_registered_model(MODEL_NAME).latest_versions
latest   = versions[-1].version
client.set_registered_model_alias(MODEL_NAME, ALIAS, latest)
print(f"\nModele version {latest} promu en '@{ALIAS}'")
