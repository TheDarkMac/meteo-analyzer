import pandas as pd
import os
import random

CSV_PATH = "data/pokemon/captured_pokemon.csv"
TOTAL_POKEMON = 1025  # À ajuster si nécessaire

def load_captured_ids():
    if not os.path.exists(CSV_PATH):
        return set()
    df = pd.read_csv(CSV_PATH)
    return set(df['numero'].tolist())

def get_random_uncaptured_ids(n, captured_ids):
    possible_ids = list(set(range(1, TOTAL_POKEMON + 1)) - captured_ids)
    return random.sample(possible_ids, min(n, len(possible_ids)))
