
import requests
from azure.identity import ClientSecretCredential
import pandas as pd
from datetime import datetime 
from update_budgets import build_budgets_data_file
from update_budgets_resources import build_budgets_resources_data_file
from update_budgets_historical_data import build_budgets_historical_data_file
from blob_utils import get_from_blob, save_to_blob
import os
from dotenv import load_dotenv



def run():
    # Charger les variables d'environnement depuis le fichier .env
    load_dotenv()
    tenant_id = os.getenv("tenant_id")
    client_id = os.getenv("client_id")
    client_secret = os.getenv("client_secret")
    billing_account_id = os.getenv("billing_account_id")

    # Vérifier si les variables sont bien chargées
    if not all([tenant_id, client_id, client_secret, billing_account_id]):
        raise ValueError("Les variables d'environnement ne sont pas correctement chargées.")

    # Azure Billing API info
    api_version = "2023-05-01"

    # Authentification avec le Service Principal
    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    access_token = credential.get_token("https://management.azure.com/.default").token
    #print(access_token)

    # URL API de l'Azure Billing
    url = f"https://management.azure.com/providers/Microsoft.Billing/billingAccounts/{billing_account_id}/providers/Microsoft.Consumption/budgets?api-version={api_version}"

    # Headers pour l'authentification
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    # Requête GET pour récupérer les budgets
    response = requests.get(url, headers=headers)

    # Vérification de la réponse
    if response.status_code == 200:
        budgets = response.json().get("value", [])
        for budget in budgets:
            print(f"Budget Name: {budget['name']}, Amount: {budget['properties']['amount']}")
    else:
        print(f"Error: {response.status_code}, {response.text}")



    try :
        build_budgets_data_file(budgets)
    except Exception as e:
        df_error_new = pd.DataFrame([{
            "date": datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
            "table": "Budgets",
            "error": e  # Contient la réponse JSON en cas d'erreur
        }])
        df_error_old = get_from_blob("error_logs.xlsx")
        df_error = pd.concat([df_error_new, df_error_old], ignore_index=True)
        save_to_blob(df_error,"error_logs.xlsx")
        print(e)


    try :
        build_budgets_historical_data_file(budgets)
    except Exception as e:
        df_error_new = pd.DataFrame([{
            "date": datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
            "table": "BudgetsHistoricalData",
            "error": e  # Contient la réponse JSON en cas d'erreur
        }])
        df_error_old = get_from_blob("error_logs.xlsx")
        df_error = pd.concat([df_error_new, df_error_old], ignore_index=True)
        save_to_blob(df_error,"error_logs.xlsx")


    try :
        build_budgets_resources_data_file(budgets)
    except Exception as e:
        df_error_new = pd.DataFrame([{
            "date": datetime.today().strftime('%Y-%m-%d %H:%M:%S'),
            "table": "BudgetsResources",
            "error": e  # Contient la réponse JSON en cas d'erreur
        }])
        df_error_old = get_from_blob("error_logs.xlsx")
        df_error = pd.concat([df_error_new, df_error_old], ignore_index=True)
        save_to_blob(df_error,"error_logs.xlsx")
        print(e)
        




if __name__ == "__main__":
    run()
