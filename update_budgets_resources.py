from datetime import datetime 
import pandas as pd
from blob_utils import get_from_blob, save_to_blob
from fill_missing_gaps import fill_missing_gaps
import os


def process_single_filter(filter_dict, base_entry):
    """
    Gère un sous-filtre qui peut contenir 'dimensions' ou 'tags'.
    Retourne une liste de dict (chaque dict représentant une ligne à ajouter).
    """
    results = []
    
    # S'il y a des "dimensions"
    if "dimensions" in filter_dict:
        dimension_name = filter_dict["dimensions"].get("name")
        dimension_values = filter_dict["dimensions"].get("values", [])
        if dimension_values:
            for value in dimension_values:
                entry = base_entry.copy()
                entry["filterDimensions.dimensions.name"] = dimension_name
                entry["filterDimensions.dimensions.values"] = value
                results.append(entry)
        else:
            entry = base_entry.copy()
            entry["filterDimensions.dimensions.name"] = dimension_name
            results.append(entry)
    
    # S'il y a des "tags"
    elif "tags" in filter_dict:
        tag_name = filter_dict["tags"].get("name")
        tag_values = filter_dict["tags"].get("values", [])
        if tag_values:
            for value in tag_values:
                entry = base_entry.copy()
                entry["filterTags.tags.name"] = tag_name
                entry["filterTags.tags.values"] = value
                results.append(entry)
        else:
            entry = base_entry.copy()
            entry["filterTags.tags.name"] = tag_name
            results.append(entry)
    
    else:
        # Cas où il n'y a ni "dimensions" ni "tags"
        # On revient à l'entrée de base
        entry = base_entry.copy()
        results.append(entry)
    
    return results



def build_budgets_resources_data_file(budgets):
    # Liste pour stocker les nouvelles données
    new_data = []
    existing_etags = set()

    old_df = get_from_blob("BudgetsResources.xlsx")
    existing_etags = set(old_df["eTag"].dropna())
    old_df = fill_missing_gaps(old_df)
    
    # Traitement des nouvelles données
    # Traitement des nouvelles données
    for budget in budgets:
        properties = budget["properties"]

        # Éviter de dupliquer des eTags déjà existants
        if budget["eTag"] in existing_etags:
            continue  # Ignorer cet enregistrement
        
        # Préparer une entrée de base
        base_entry = {
            "name": budget["name"],
            "eTag": budget["eTag"].strip('"'),
            "filterDimensions.dimensions.name": None,
            "filterDimensions.dimensions.values": None,
            "filterTags.tags.name": None,
            "filterTags.tags.values": None
        }
        
        # Gestion du filtre (dimensions/tags/and)
        if "filter" in properties:
            filter_data = properties["filter"]
            if "and" in filter_data:
                # S'il y a un tableau "and", on boucle sur chaque sous-filtre
                for sub_filter in filter_data["and"]:
                    new_data_entries = process_single_filter(sub_filter, base_entry)
                    new_data.extend(new_data_entries)
            else:
                # Cas plus simple sans "and"
                new_data_entries = process_single_filter(filter_data, base_entry)
                new_data.extend(new_data_entries)
        else:
            # S'il n'y a pas de "filter"
            new_data.append(base_entry)
    
    # Création d'un DataFrame pour les nouvelles données
    today_date = datetime.today().strftime('%Y-%m-%d')
    df_new = pd.DataFrame(new_data)
    df_new["Date"] = today_date
    df_new['extraction_datetime'] = datetime.now()
    # Fusionner avec l'ancien DataFrame
    final_df = pd.concat([old_df, df_new], ignore_index=True)


    final_df['Date'] = pd.to_datetime(final_df['Date']).dt.strftime('%Y-%m-%d')

    # Sauvegarde sous format Excel
    save_to_blob(final_df, "BudgetsResources.xlsx")
    final_df.to_excel(f"{os.getcwd()}/archive/BudgetsResources_{datetime.today().strftime('%Y-%m-%d')}.xlsx", index=False)
    
    print(f"Fichier Excel mis à jour sous BudgetsResources.xlsx")



    
