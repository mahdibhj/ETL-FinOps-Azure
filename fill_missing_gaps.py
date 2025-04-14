import pandas as pd
from datetime import datetime, timedelta

def fill_missing_gaps(df):
    
    # Convertir la colonne Date en datetime
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Trouver la date maximale existante
    max_date = df['Date'].max()
    
    # Déterminer la date d'hier
    yesterday = datetime.today().date() - timedelta(days=1)
    
    # Vérifier si la date maximale est inférieure à hier
    if max_date.date() < yesterday:
        
        # Filtrer les lignes correspondant à la date maximale
        max_date_rows = df[df['Date'] == max_date]
        
        # Générer les dates manquantes
        missing_dates = pd.date_range(start=max_date + timedelta(days=1), end=yesterday)
        
        # Copier les données pour chaque date manquante
        new_rows = []
        for missing_date in missing_dates:
            temp_df = max_date_rows.copy()
            temp_df['Date'] = missing_date
            temp_df['extraction_datetime'] = datetime.now()
            new_rows.append(temp_df)
        
        # Ajouter les nouvelles lignes au dataframe
        df = pd.concat([df] + new_rows, ignore_index=True)
        
        # retourner le df final
        print(f"Mise à jour effectuée")
        return df
        
    else:
        return df
        print("Aucune mise à jour nécessaire.")


