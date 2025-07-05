import os
import requests
import pandas as pd
from datetime import datetime
import logging


def extract_meteo(city: str, api_key: str, date: str) -> bool:
    """
    Extrait les données météo pour une ville via l'API OpenWeather

    Args:
        city (str): Nom de la ville à interroger
        api_key (str): Clé d'API OpenWeather
        date (str): Date au format 'YYYY-MM-DD' pour l'organisation

    Returns:
        bool: True si l'extraction réussit, False sinon

    Exemple:
        >>> extract_meteo("Paris", "abc123", "2025-06-01")
    """
    try:
        # Configuration de la requête API
        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {
            'q': city,
            'appid': api_key,
            'units': 'metric',  # Température en Celsius
            'lang': 'fr'  # Description en français
        }

        # Envoi de la requête avec timeout de 10s
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()  # Vérifie les erreurs HTTP

        # Extraction des champs pertinents
        data = response.json()
        weather_data = {
            'ville': city,
            'date_extraction': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'temperature': data['main']['temp'],
            'humidite': data['main']['humidity'],
            'pression': data['main']['pressure'],
            'description': data['weather'][0]['description']
        }

        # Création du dossier de destination
        os.makedirs(f"data/raw/{date}", exist_ok=True)

        # Sauvegarde en CSV
        pd.DataFrame([weather_data]).to_csv(
            f"data/raw/{date}/meteo_{city}.csv",
            index=False  # Pas d'enregistrement de l'index
        )

        return True

    except requests.exceptions.RequestException as e:
        logging.error(f"Erreur réseau/API pour {city}: {str(e)}")
    except KeyError as e:
        logging.error(f"Champ manquant dans la réponse pour {city}: {str(e)}")
    except Exception as e:
        logging.error(f"Erreur inattendue pour {city}: {str(e)}")

    return False