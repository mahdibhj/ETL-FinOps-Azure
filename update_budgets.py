from datetime import datetime, timedelta
import pandas as pd
from blob_utils import get_from_blob, save_to_blob
import os 


def build_budgets_data_file(budgets):
        #transformation des données
        budget_data = []
        today = datetime.today().date()  # Obtenir la date d'aujourd'hui
        
        for budget in budgets:
            properties = budget['properties']
            start_date = datetime.strptime(properties['timePeriod']['startDate'], "%Y-%m-%dT%H:%M:%SZ").date()
        
            if start_date <= today:  # Filtrer les budgets en fonction de la date de début
                budget_data.append({
                    'name': budget['name'],
                    'type': budget['type'],
                    'eTag': budget['eTag'].strip('"'),
                    'startDate': properties['timePeriod']['startDate'],
                    'endDate': properties['timePeriod']['endDate'],
                    'timeGrain': properties['timeGrain'],
                    'budgetAmount': properties['amount'],
                    'currentSpendAmount': properties['currentSpend']['amount'],
                    'currentSpendUnit': properties['currentSpend']['unit'],
                    'category': properties['category'],
                    'forecastAmount': properties.get('forecastSpend', {}).get('amount', 0.0),
                    'forecastUnit': properties.get('forecastSpend', {}).get('unit', 'CAD')
                })
        
        # Création du DataFrame à partir des nouveaux budgets
        df_new = pd.DataFrame(budget_data)
        
        # Conversion des dates au bon format
        df_new['startDate'] = pd.to_datetime(df_new['startDate']).dt.strftime('%Y-%m-%d')
        df_new['endDate'] = pd.to_datetime(df_new['endDate']).dt.strftime('%Y-%m-%d')
        
        # Charger l'ancien fichier s'il existe
        #try : 
        df_existing = get_from_blob("Budgets.xlsx")
        df_existing['startDate'] = pd.to_datetime(df_existing['startDate'])
        df_existing['endDate'] = pd.to_datetime(df_existing['endDate'])
        
        # Filtrer les nouveaux budgets en excluant ceux dont l'eTag est déjà présent
        existing_eTags = set(df_existing['eTag'])
        df_new = df_new[~df_new['eTag'].isin(existing_eTags)]
        
        if not df_new.empty:
            today = datetime.today()
            yesterday = today - timedelta(days=1)
            
            for index, row in df_new.iterrows():
                matching_records = df_existing[df_existing['name'] == row['name']]
                if not matching_records.empty:
                    # Trouver le record avec la date de fin la plus récente
                    last_record_idx = matching_records['endDate'].idxmax()
                    df_existing.at[last_record_idx, 'endDate'] = yesterday.strftime('%Y-%m-%d')
                    # Mettre la nouvelle date de début au record courant
                    df_new.at[index, 'startDate'] = today.strftime('%Y-%m-%d')
            
            # Fusionner les anciens et les nouveaux budgets
            df_final = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            df_final = df_existing
        
        df_final['startDate'] = pd.to_datetime(df_final['startDate']).dt.strftime('%Y-%m-%d')
        df_final['endDate'] = pd.to_datetime(df_final['endDate']).dt.strftime('%Y-%m-%d')
        # Enregistrement du fichier Excel
        save_to_blob(df_final, "Budgets.xlsx")

        df_final.to_excel(f"{os.getcwd()}/archive/Budgets_{datetime.today().strftime('%Y-%m-%d')}.xlsx", index=False)
        
    
        

