import pandas as pd
import os

CSV_PATH = "data/pokemon/captured_pokemon.csv"

def save_to_csv(pokemon_list):
    df_new = pd.DataFrame(pokemon_list)
    
    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    
    if os.path.exists(CSV_PATH):
        df_old = pd.read_csv(CSV_PATH)
        df_all = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_all = df_new
    df_all.to_csv(CSV_PATH, index=False)
