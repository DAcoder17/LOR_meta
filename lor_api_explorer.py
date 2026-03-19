"""
=============================================================
  Legends of Runeterra - API Explorer
  Exploración inicial de endpoints de Riot Games
=============================================================
  Configuración:
    1. Obtén tu API Key en: https://developer.riotgames.com
    2. Reemplaza API_KEY con tu clave
    3. Ajusta PUUID con el tuyo (ver cómo obtenerlo abajo)
=============================================================
"""

import requests
import json
from pprint import pprint

# ─────────────────────────────────────────────
# CONFIGURACIÓN INICIAL
# ─────────────────────────────────────────────

API_KEY = "RGAPI-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"  # ← Reemplaza aquí

# Regiones de routing para LOR
REGIONS = {
    "americas": "americas.api.riotgames.com",
    "europe":   "europe.api.riotgames.com",
    "sea":      "sea.api.riotgames.com",
}

# Región a usar (cambia según tu servidor)
REGION = "americas"
BASE_URL = f"https://{REGIONS[REGION]}"

HEADERS = {
    "X-Riot-Token": API_KEY
}


# ─────────────────────────────────────────────
# UTILIDADES
# ─────────────────────────────────────────────

def get(endpoint: str, params: dict = None) -> dict | list | None:
    """Realiza un GET request y retorna el JSON o imprime el error."""
    url = f"{BASE_URL}{endpoint}"
    print(f"\n→ GET {url}")
    response = requests.get(url, headers=HEADERS, params=params)
    print(f"  Status: {response.status_code}")
    if response.status_code == 200:
        return response.json()
    else:
        print(f"  Error: {response.text}")
        return None


def print_schema(data, label="Respuesta"):
    """Imprime estructura y primeros valores de la respuesta."""
    print(f"\n{'─'*50}")
    print(f"  {label}")
    print(f"{'─'*50}")
    if data is None:
        print("  Sin datos.")
        return
    if isinstance(data, list):
        print(f"  Tipo: lista  |  Elementos: {len(data)}")
        if len(data) > 0:
            print(f"  Primer elemento:")
            pprint(data[0], indent=4)
    elif isinstance(data, dict):
        print(f"  Tipo: objeto  |  Claves: {list(data.keys())}")
        pprint(data, indent=4)
    print()


# ─────────────────────────────────────────────
# 1. LOR-RANKED-V1 — Leaderboard
#    No requiere PUUID, acceso directo
# ─────────────────────────────────────────────

def explore_ranked():
    print("\n" + "═"*50)
    print("  LOR-RANKED-V1  /  Leaderboard")
    print("═"*50)
    data = get("/lor/ranked/v1/leaderboards")
    print_schema(data, "Leaderboard completo")

    if data and "players" in data:
        print(f"  Total jugadores en leaderboard: {len(data['players'])}")
        print("\n  Columnas disponibles por jugador:")
        if data["players"]:
            pprint(list(data["players"][0].keys()), indent=4)
            print("\n  Ejemplo de jugador:")
            pprint(data["players"][0], indent=4)


# ─────────────────────────────────────────────
# 2. LOR-STATUS-V1 — Estado de la plataforma
# ─────────────────────────────────────────────

def explore_status():
    print("\n" + "═"*50)
    print("  LOR-STATUS-V1  /  Platform Data")
    print("═"*50)
    data = get("/lor/status/v1/platform-data")
    print_schema(data, "Estado de la plataforma")


# ─────────────────────────────────────────────
# 3. ACCOUNT-V1 — Obtener PUUID a partir de
#    gameName + tagLine (útil para comenzar)
# ─────────────────────────────────────────────

def get_puuid(game_name: str, tag_line: str) -> str | None:
    print("\n" + "═"*50)
    print(f"  ACCOUNT-V1  /  {game_name}#{tag_line}")
    print("═"*50)
    data = get(f"/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}")
    print_schema(data, "Datos de cuenta")
    if data:
        return data.get("puuid")
    return None


# ─────────────────────────────────────────────
# 4. LOR-MATCH-V1 — Lista de partidas por PUUID
# ─────────────────────────────────────────────

def explore_match_ids(puuid: str, count: int = 5):
    print("\n" + "═"*50)
    print("  LOR-MATCH-V1  /  Match IDs")
    print("═"*50)
    data = get(
        f"/lor/match/v1/matches/by-puuid/{puuid}/ids",
        params={"count": count}
    )
    print_schema(data, f"IDs de las últimas {count} partidas")
    return data if data else []


# ─────────────────────────────────────────────
# 5. LOR-MATCH-V1 — Detalle de una partida
# ─────────────────────────────────────────────

def explore_match_detail(match_id: str):
    print("\n" + "═"*50)
    print(f"  LOR-MATCH-V1  /  Match Detail: {match_id}")
    print("═"*50)
    data = get(f"/lor/match/v1/matches/{match_id}")
    if data is None:
        return

    # Metadata
    print("\n  ── metadata ──")
    pprint(data.get("metadata", {}), indent=4)

    # Info general
    info = data.get("info", {})
    print("\n  ── info (claves disponibles) ──")
    print(f"  {list(info.keys())}")

    # Participantes
    players = info.get("players", [])
    print(f"\n  ── players ({len(players)} jugadores) ──")
    for i, p in enumerate(players):
        print(f"\n  Jugador {i+1}:")
        pprint(p, indent=6)

        # Desglosa el mazo si existe
        deck = p.get("deck_id") or p.get("deck_code")
        if deck:
            print(f"    → deck_id/deck_code: {deck}")

    return data


# ─────────────────────────────────────────────
# 6. LOR-DECK-V1 — Mazos del jugador (RSO)
#    Nota: requiere autenticación OAuth (RSO),
#    no funciona con Development API Key sola
# ─────────────────────────────────────────────

def explore_decks_info():
    print("\n" + "═"*50)
    print("  LOR-DECK-V1  /  Decks (RSO only)")
    print("═"*50)
    print("""
  ⚠  Este endpoint requiere autenticación RSO (OAuth 2.0).
     No es accesible con una Development API Key estándar.

  Para usarlo necesitas:
    1. Registrar tu app en el portal de Riot
    2. Implementar el flujo OAuth 2.0 (Authorization Code)
    3. Obtener un access_token del usuario

  Estructura esperada de la respuesta:
  [
    {
      "id":         str,   # ID único del mazo
      "name":       str,   # Nombre del mazo
      "code":       str,   # Código exportable del mazo
    },
    ...
  ]
    """)


# ─────────────────────────────────────────────
# 7. LOR-INVENTORY-V1 — Inventario (RSO)
# ─────────────────────────────────────────────

def explore_inventory_info():
    print("\n" + "═"*50)
    print("  LOR-INVENTORY-V1  /  Cards (RSO only)")
    print("═"*50)
    print("""
  ⚠  Este endpoint también requiere autenticación RSO.

  Estructura esperada de la respuesta:
  [
    {
      "itemID":    int,   # ID de la carta
      "assets": [
        {
          "gameAbsolutePath": str,  # URL imagen del juego
          "fullAbsolutePath":  str,  # URL imagen full art
        }
      ]
    },
    ...
  ]
    """)


# ─────────────────────────────────────────────
# EJECUCIÓN PRINCIPAL
# ─────────────────────────────────────────────

if __name__ == "__main__":

    print("""
╔══════════════════════════════════════════════╗
║   LOR API Explorer — Exploración Inicial     ║
╚══════════════════════════════════════════════╝
    """)

    # ── Endpoints sin PUUID (acceso directo) ──
    explore_ranked()
    explore_status()

    # ── Info sobre endpoints RSO ──
    explore_decks_info()
    explore_inventory_info()

    # ── Con PUUID: reemplaza con tu gameName#tag ──
    # Ejemplo: "NombreJugador", "LAN"
    GAME_NAME = "TuNombre"   # ← Reemplaza
    TAG_LINE  = "LAN"        # ← Reemplaza (NA1, LAN, EUW, etc.)

    puuid = get_puuid(GAME_NAME, TAG_LINE)

    if puuid:
        print(f"\n  ✓ PUUID obtenido: {puuid[:20]}...")
        match_ids = explore_match_ids(puuid, count=3)

        if match_ids:
            print(f"\n  → Explorando detalle de la primera partida...")
            explore_match_detail(match_ids[0])
    else:
        print("\n  ✗ No se pudo obtener el PUUID. Verifica tu API Key y gameName#tag.")

    print("\n\n  ✓ Exploración finalizada.\n")
