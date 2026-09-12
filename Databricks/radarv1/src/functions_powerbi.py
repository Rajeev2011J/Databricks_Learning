import requests

def refresh_pbi_semantic_model(access_token: str, group_id: str, dataset_id: str) -> None:
    """
    Refresh a Power BI semantic model
    """
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets/{dataset_id}/refreshes"

    headers = { 
        "Auhorization": f"Bearer {access_token}", 
        "Content-Type": "application/json",
    }

    try:
        response = requests.post(url, headers=headers, timeout=10)

        if response.status_code == 202:
            print("PBI semantic model refresh started successfully.")
        elif response.status_code == 401:
            print("PBI semantic model refresh failed with status code 401. Unauthorized access.")
        else:
            print(f"Failed to start PBI semantic model refresh: {response.status_code} - {response.text}")

    except requests.exceptions.Timeout:
        print("Request to start PBI semantic model resfresh timed out")