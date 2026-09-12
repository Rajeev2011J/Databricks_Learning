import os
import json
import requests
import mlflow.pyfunc

class OpenAIProxyModel(mlflow.pyfunc.PythonModel):
    """A tiny MLflow pyfunc that forwards incoming JSON to an Azure OpenAI proxy
    using Databricks secrets for authentication and returns the raw JSON response.

    Expected input (flexible):
    - a pandas DataFrame with a single row containing either:
        - a column `payload` (dict) and optional `endpoint` (string), or
        - a single column whose dict value will be used as payload.
    - a dict with `payload` and optional `endpoint`.

    If `endpoint` is omitted, the model forwards to:
      {BASE_URL}openai/deployments/{DEPLOYMENT_NAME}/chat/completions?api-version={AZURE_OPENAI_VERSION}
    """

    def load_context(self, context):
        # Configuration
        LAB_VARIANT = "OpenLab"
        DEPLOYMENT_NAME = "gpt-4o"
        ENVIRONMENT = "prd"

        # import dbutils and Azure credential inside runtime
        from azure.identity import ClientSecretCredential

        secret_scope = f"{LAB_VARIANT}-SecretScope"

        client_id = dbutils.secrets.get(scope=secret_scope, key="DataServicePrincipalClientId")
        client_secret = dbutils.secrets.get(scope=secret_scope, key="DataServicePrincipalClientSecret")
        tenant_id = "6e93a626-8aca-4dc1-9191-ce291b4b75a1"

        credential = ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)

        # Build base URL from secret (matches user's get_openai_urls behavior for OpenLab)
        hostname = dbutils.secrets.get(scope=secret_scope, key="OpenAiHostname")
        base_url = f"https://{hostname}openoaisdc-completions-apis/"

        # Get access token
        access_token = credential.get_token("https://cognitiveservices.azure.com/.default")

        # Save to instance for use during predict
        self.base_url = base_url
        self.token = access_token.token
        self.api_version = os.environ.get("AZURE_OPENAI_VERSION", "2024-02-01")
        self.deployment_name = DEPLOYMENT_NAME

    def _build_url(self, endpoint: str | None) -> str:
        # If caller provided a full URL, use it directly
        if endpoint:
            if endpoint.startswith("http://") or endpoint.startswith("https://"):
                return endpoint
            # otherwise join to base_url
            return self.base_url.rstrip("/") + "/" + endpoint.lstrip("/")

        # Default to chat completions using configured deployment
        return f"{self.base_url}openai/deployments/{self.deployment_name}/chat/completions?api-version={self.api_version}"

    def predict(self, context, model_input):
        """Accepts various JSON-like inputs, forwards to the OpenAI proxy and returns DataFrame with `response` column."""
        import pandas as pd

        # Normalize input to a dict (row)
        if isinstance(model_input, pd.DataFrame):
            if model_input.shape[0] < 1:
                raise ValueError("Input DataFrame must contain at least one row")
            # Use first row
            row = model_input.iloc[0].to_dict()
        elif isinstance(model_input, dict):
            row = model_input
        elif isinstance(model_input, str):
            try:
                row = json.loads(model_input)
            except Exception as e:
                raise ValueError("String input must be valid JSON") from e
        else:
            # attempt to coerce
            try:
                row = dict(model_input)
            except Exception:
                raise ValueError("Unsupported input type for pyfunc model")

        # payload: if user passed a wrapper with `payload` key, use that; otherwise use the whole row
        payload = row.get("payload", row)
        endpoint = row.get("endpoint")

        url = self._build_url(endpoint)

        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

        resp = requests.post(url, headers=headers, json=payload, timeout=120)
        resp.raise_for_status()
        result = resp.json()

        # Return as DataFrame with a single column `response` containing the parsed JSON
        return pd.DataFrame([{"response": result}])
