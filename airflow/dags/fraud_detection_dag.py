"""
Bloc 3 — Automatic Fraud Detection
DAG Airflow : pipeline de détection de fraude en temps réel

Flux :
    fetch_data → validate_schema → infer_fraud → store_predictions

- fetch_data        : récupère les transactions depuis l'API temps réel
- validate_schema   : valide la qualité des données (Great Expectations)
- infer_fraud       : charge le modèle depuis MLflow et produit les prédictions
- store_predictions : enregistre les résultats dans NeonDB (PostgreSQL)

Déclenchement : @hourly (configurable)
"""

from __future__ import annotations

import os
import signal
from datetime import datetime, timedelta
from io import StringIO

import mlflow.pyfunc
import pandas as pd
import psycopg2
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from psycopg2.extras import execute_values

# ── Config ────────────────────────────────────────────────────────────────────
API_URL     = "https://sdacelo-real-time-fraud-detection.hf.space/current-transactions"
MLFLOW_URI  = Variable.get("MLFLOW_TRACKING_URI", default_var="https://atomik31-mlflow.hf.space")
MODEL_NAME  = Variable.get("MODEL_NAME",  default_var="fraud_detector")
MODEL_ALIAS = Variable.get("MODEL_ALIAS", default_var="production")
NEON_CONN   = Variable.get("NEON_CONN")

REQUIRED_COLS = [
    "cc_num", "category", "amt", "gender", "city", "state",
    "zip", "lat", "long", "city_pop", "job", "dob",
    "trans_num", "merch_lat", "merch_long", "current_time",
]

default_args = {
    "owner":        "airflow",
    "retries":      1,
    "retry_delay":  timedelta(minutes=5),
    "email_on_failure": False,
}


#  TÂCHE 1 — Récupération des données

def fetch_data(**context):
    """Appelle l'API temps réel et pousse le DataFrame brut dans XCom."""
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    df = pd.read_json(StringIO(response.json()), orient="split")
    print(f"[fetch_data] {len(df)} transaction(s) recuperee(s).")
    context["ti"].xcom_push(key="raw_data", value=df.to_json())


#  TÂCHE 2 — Validation du schéma (Great Expectations)

def validate_schema(**context):
    """Vérifie colonnes requises, nullité et montant positif."""
    import great_expectations as gx

    def _timeout(signum, frame):
        raise TimeoutError("validate_schema timeout (GX context)")

    signal.signal(signal.SIGALRM, _timeout)
    signal.alarm(60)

    try:
        raw_json = context["ti"].xcom_pull(key="raw_data", task_ids="fetch_data")
        df = pd.read_json(StringIO(raw_json))

        ctx   = gx.get_context(mode="ephemeral")
        ds    = ctx.data_sources.add_pandas("api_source")
        asset = ds.add_dataframe_asset("transactions")
        batch_def = asset.add_batch_definition_whole_dataframe("batch")

        suite = ctx.suites.add(gx.ExpectationSuite(name="fraud_model_input"))

        suite.add_expectation(
            gx.expectations.ExpectTableColumnsToMatchSet(
                column_set=REQUIRED_COLS, exact_match=False
            )
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column="current_time")
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="amt", min_value=0, strict_min=True
            )
        )

        val_def = ctx.validation_definitions.add(
            gx.ValidationDefinition(name="fraud_validation", data=batch_def, suite=suite)
        )
        results = val_def.run(batch_parameters={"dataframe": df})

        if not results.success:
            failed = [
                r["expectation_type"]
                for r in results.to_json_dict()["results"]
                if not r["success"]
            ]
            raise ValueError(f"Validation GX echouee : {failed}")

        print(f"[validate_schema] OK — {len(df)} ligne(s) validee(s).")

    finally:
        signal.alarm(0)


#  TÂCHE 3 — Inférence via le modèle MLflow

def infer_fraud(**context):
    """
    Charge le modèle depuis le MLflow Model Registry (@production)
    et produit les prédictions de fraude.
    Effectue un ping de réveil du Space HuggingFace avant le chargement.
    """
    raw_json = context["ti"].xcom_pull(key="raw_data", task_ids="fetch_data")
    df = pd.read_json(StringIO(raw_json))

    # Ping de réveil : le Space HuggingFace peut être en cold start
    mlflow_base = MLFLOW_URI.rstrip("/")
    try:
        ping = requests.get(mlflow_base, timeout=90)
        print(f"[infer_fraud] MLflow Space ping : {ping.status_code}")
    except Exception as e:
        print(f"[infer_fraud] Ping warning (non bloquant) : {e}")

    mlflow.set_tracking_uri(MLFLOW_URI)
    model_uri = f"models:/{MODEL_NAME}@{MODEL_ALIAS}"
    model = mlflow.pyfunc.load_model(model_uri)
    print(f"[infer_fraud] Modele charge : {model_uri}")

    # Alignement API → schéma du modèle (API retourne current_time, modèle attend trans_date_trans_time)
    if "current_time" in df.columns and "trans_date_trans_time" not in df.columns:
        df = df.rename(columns={"current_time": "trans_date_trans_time"})
    if "trans_date_trans_time" in df.columns:
        df["trans_date_trans_time"] = df["trans_date_trans_time"].astype(str)

    extra_cols = ["first", "last", "street", "merchant", "is_fraud", "unix_time"]
    df = df.drop(columns=[c for c in extra_cols if c in df.columns])

    predictions = model.predict(df)
    df["is_fraud_predicted"] = predictions
    df["predicted_at"] = datetime.utcnow().isoformat()

    fraud_count = int(sum(predictions))
    print(f"[infer_fraud] {len(df)} transaction(s) scoree(s) | {fraud_count} fraude(s).")

    context["ti"].xcom_push(key="predictions", value=df.to_json())


#  TÂCHE 4 — Stockage dans NeonDB

def store_predictions(**context):
    """Insère les prédictions dans la table fraud_predictions de NeonDB."""
    predictions_json = context["ti"].xcom_pull(key="predictions", task_ids="infer_fraud")
    df = pd.read_json(StringIO(predictions_json))

    conn   = psycopg2.connect(NEON_CONN)
    cursor = conn.cursor()

    dtype_map = {
        "int64":          "BIGINT",
        "float64":        "DOUBLE PRECISION",
        "bool":           "BOOLEAN",
        "object":         "TEXT",
        "datetime64[ns]": "TIMESTAMP",
    }
    col_defs = ", ".join(
        f'"{col}" {dtype_map.get(str(df[col].dtype), "TEXT")}'
        for col in df.columns
    )
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS fraud_predictions (
            id SERIAL PRIMARY KEY,
            {col_defs}
        );
    """)

    cols = ", ".join(f'"{c}"' for c in df.columns)
    execute_values(
        cursor,
        f"INSERT INTO fraud_predictions ({cols}) VALUES %s",
        df.values.tolist(),
    )

    conn.commit()
    cursor.close()
    conn.close()

    fraud_count = int(df["is_fraud_predicted"].sum())
    print(f"[store_predictions] {len(df)} ligne(s) inseree(s) | {fraud_count} fraude(s).")


#  DAG

with DAG(
    dag_id="fraud_detection_pipeline",
    default_args=default_args,
    description="Fetch → Validate → Infer → Store — detection de fraude temps reel",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["fraud", "mlflow", "neondb", "great_expectations"],
) as dag:

    t_fetch = PythonOperator(task_id="fetch_data",        python_callable=fetch_data)
    t_validate = PythonOperator(task_id="validate_schema", python_callable=validate_schema)
    t_infer = PythonOperator(task_id="infer_fraud",        python_callable=infer_fraud)
    t_store = PythonOperator(task_id="store_predictions",  python_callable=store_predictions)

    t_fetch >> t_validate >> t_infer >> t_store
