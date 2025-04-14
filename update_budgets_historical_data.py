
from datetime import datetime 
import pandas as pd
from blob_utils import get_from_blob, save_to_blob
from fill_missing_gaps import fill_missing_gaps
import os



def build_budgets_historical_data_file(budgets):
        # Transformation des données
        budget_data = []
        for budget in budgets:
            properties = budget['properties']
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
        
        # Création du DataFrame Pandas
        df_new = pd.DataFrame(budget_data)
        
        # Conversion des dates au bon format
        df_new['startDate'] = pd.to_datetime(df_new['startDate']).dt.strftime('%Y-%m-%d')
        df_new['endDate'] = pd.to_datetime(df_new['endDate']).dt.strftime('%Y-%m-%d')
        df_new['extraction_datetime'] = datetime.now()
        df_new['Date'] = datetime.now().strftime('%Y-%m-%d')
        # Enregistrement au format Excel
        #file_path = "budgets.xlsx"
        df_old = get_from_blob("BudgetsHistoricalData.xlsx")
        df_old = fill_missing_gaps(df_old)
        df = pd.concat([df_new, df_old], ignore_index=True)
        df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
        #print(df)
        save_to_blob(df, "BudgetsHistoricalData.xlsx")
        df.to_excel(f"{os.getcwd()}/archive/BudgetsHistoricalData_{datetime.today().strftime('%Y-%m-%d')}.xlsx", index=False)
        print(f"Fichier enregistré sous : Blob storage")


    
        