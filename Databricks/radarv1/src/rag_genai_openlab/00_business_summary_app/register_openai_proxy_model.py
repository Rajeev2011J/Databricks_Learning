# Databricks notebook source
import os
import time
import mlflow
from mlflow.tracking import MlflowClient


def _get_dbutils():
    try:
        # Preferred in Databricks runtime
        from databricks.sdk.runtime import dbutils
        return dbutils
    except Exception:
        # Fallback to notebook-global dbutils (when run inside Databricks notebook)
        return globals().get("dbutils")


def register_pyfunc_model(model_name: str = "openai_proxy_pyfunc", pip_reqs=None, timeout_seconds: int = 180):
    """Log the `OpenAIProxyModel` as a pyfunc, ensure artifacts live on DBFS,
    and register the model in the Databricks Model Registry.

    If MLflow returns a non-DBFS artifact URI, this function downloads artifacts
    locally and copies them to DBFS under `/tmp/mlflow_artifacts/<run_id>/model`.
    """
    if pip_reqs is None:
        pip_reqs = [
            "mlflow",
            "databricks-sdk",
            "azure-identity",
            "requests",
            "pandas",
        ]

    from openai_proxy_model import OpenAIProxyModel

    # 1) Start MLflow run and log pyfunc
    with mlflow.start_run() as run:
        run_id = run.info.run_id
        print(f"Starting MLflow run: {run_id}")

        mlflow.pyfunc.log_model(
            artifact_path="model",
            python_model=OpenAIProxyModel(),
            pip_requirements=pip_reqs,
        )

        # Get artifact URI
        artifact_uri = mlflow.get_artifact_uri("model")
        print(f"mlflow.get_artifact_uri('model') = {artifact_uri}")

    # 2) Ensure we have a DBFS path for the artifact
    dbutils = _get_dbutils()
    if dbutils is None:
        raise RuntimeError("`dbutils` is not available. Run this in a Databricks notebook or ensure databricks.sdk.runtime.dbutils is importable.")

    if artifact_uri.startswith("dbfs:/"):
        artifact_dbfs_uri = artifact_uri
        print(f"Artifact already on DBFS: {artifact_dbfs_uri}")
    else:
        # Download artifacts locally and copy to DBFS
        print("Artifact URI is not DBFS. Downloading artifacts locally and copying to DBFS...")
        local_dir = mlflow.artifacts.download_artifacts(artifact_uri=artifact_uri)
        print(f"Downloaded artifacts to local path: {local_dir}")

        # Choose a target DBFS path to upload artifacts to
        target_dbfs_base = f"dbfs:/tmp/mlflow_artifacts/{run_id}"
        artifact_dbfs_uri = f"{target_dbfs_base}/model"

        # Create parent dir
        try:
            dbutils.fs.mkdirs(target_dbfs_base)
        except Exception as e:
            print(f"Warning: mkdirs on DBFS failed: {e}")

        # Copy entire local folder to DBFS using dbutils.fs.cp with file: scheme
        # dbutils.fs.cp supports copying a local `file:` path into DBFS with recurse=True
        src = f"file:{local_dir}"
        dst = artifact_dbfs_uri
        try:
            # Attempt a recursive copy
            dbutils.fs.cp(src, dst, recurse=True)
            print(f"Copied local artifacts to DBFS at: {artifact_dbfs_uri}")
        except Exception as e:
            # Fall back to manual upload of files
            print(f"dbutils.fs.cp failed ({e}), falling back to per-file upload...")
            for root, _, files in os.walk(local_dir):
                rel_root = os.path.relpath(root, local_dir)
                rel_root = "" if rel_root == "." else rel_root
                dbfs_dir = artifact_dbfs_uri if rel_root == "" else f"{artifact_dbfs_uri}/{rel_root}"
                try:
                    dbutils.fs.mkdirs(dbfs_dir)
                except Exception:
                    pass
                for fname in files:
                    local_path = os.path.join(root, fname)
                    dbfs_path = f"{dbfs_dir}/{fname}"
                    try:
                        dbutils.fs.cp(f"file:{local_path}", dbfs_path)
                    except Exception:
                        # As a last resort, read and write via put (text mode)
                        with open(local_path, "rb") as f:
                            data = f.read()
                        # dbutils.fs.put expects string; write binary by encoding as latin1
                        try:
                            dbutils.fs.put(dbfs_path, data.decode("latin1"), overwrite=True)
                        except Exception as ee:
                            print(f"Failed to upload {local_path} -> {dbfs_path}: {ee}")

    # 3) Register model using DBFS artifact path
    client = MlflowClient()
    try:
        client.create_registered_model(model_name)
        print(f"Created registered model: {model_name}")
    except Exception as e:
        print(f"Registered model `{model_name}` may already exist: {e}")

    mv = client.create_model_version(name=model_name, source=artifact_dbfs_uri, run_id=run_id)
    print(f"Created model version {mv.version} (initial status={mv.status})")

    # 4) Wait for READY
    poll_interval = 3
    elapsed = 0
    while elapsed < timeout_seconds:
        mv = client.get_model_version(model_name, mv.version)
        status = mv.status
        print(f"Model version {mv.version} status: {status}")
        if status == "READY":
            print(f"Model `{model_name}` version {mv.version} is READY")
            break
        if status == "FAILED":
            raise RuntimeError(f"Model registration failed: {mv}")
        time.sleep(poll_interval)
        elapsed += poll_interval
    else:
        print("Timed out waiting for model to become READY. Check the model version status in the Model Registry.")

    print(f"Registered model URI (used as source): {artifact_dbfs_uri}")
    return model_name, mv.version


if __name__ == "__main__":
    name, version = register_pyfunc_model()
    print(f"Done. Registered {name} version {version}")

