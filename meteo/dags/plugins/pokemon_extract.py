import requests

def fetch_pokemon_data(pokemon_id):
    url = f"https://pokeapi.co/api/v2/pokemon/{pokemon_id}"
    resp = requests.get(url)
    if resp.status_code != 200:
        return None
    data = resp.json()
    return {
        "numero": data["id"],
        "nom": data["name"],
        "poids": data["weight"],
        "taille": data["height"],
        "types": ",".join([t["type"]["name"] for t in data["types"]]),
        "url_image": data["sprites"]["other"]["dream_world"]["front_default"],
        "location_area_encounters": data["location_area_encounters"]
    }
