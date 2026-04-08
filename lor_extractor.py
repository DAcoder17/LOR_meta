"""
=============================================================
  LOR — Extracción periódica del Top 1750 Maestros
=============================================================
  Para cada jugador del leaderboard:
    1. Obtiene sus últimas 10 partidas
    2. Identifica el mazo más usado
    3. Decodifica el deck_code → lista de cartas
    4. Guarda/actualiza lor_dataset.csv  +  lor_dataset.xlsx

  Diseñado para correr en VS Code de forma local.
  Puede interrumpirse y retomarse gracias al checkpoint.

  Uso:
    python lor_extractor.py

  Requisitos:
    pip install requests openpyxl pandas
=============================================================
"""

import time
import json
import requests
import pandas as pd
from pathlib import Path
from collections import Counter
from datetime import datetime, timezone

# ══════════════════════════════════════════════════════════════
# CONFIGURACIÓN  —  edita solo esta sección
# ══════════════════════════════════════════════════════════════

API_KEY       = "RGAPI-xxxx-xxxx-xxxx-xxxx"   # ← tu clave actual
REGION        = "americas"                      # americas | europe | sea
TOP_N         = 1750                            # jugadores a procesar
MATCHES_COUNT = 10                              # últimas N partidas por jugador
DELAY         = 1.5                             # segundos entre requests
MAX_PENALTY   = 120                             # Retry-After > esto → detener

OUTPUT_CSV    = Path("lor_dataset.csv")
OUTPUT_XLSX   = Path("lor_dataset.xlsx")
CHECKPOINT    = Path("lor_checkpoint.json")

CANDIDATE_TAGS = ["NA1", "NA", "LAN", "LAS", "BR1", "BR",
                  "1", "001", "OCE1", "OCE", "1234", "0"]

# ══════════════════════════════════════════════════════════════
# DECODIFICADOR DE DECK CODES  (spec abierta de Riot)
# ══════════════════════════════════════════════════════════════

B32 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"

FACTION_ID = {
    0:"DE", 1:"FR", 2:"IO", 3:"NX", 4:"PZ",
    5:"SI", 6:"BW", 7:"SH", 9:"MT", 10:"BC", 12:"RU"
}
FACTION_NAME = {
    "DE":"Demacia",      "FR":"Freljord",       "IO":"Ionia",
    "NX":"Noxus",        "PZ":"Piltover & Zaun","SI":"Shadow Isles",
    "BW":"Bilgewater",   "SH":"Shurima",        "MT":"Mount Targon",
    "BC":"Bandle City",  "RU":"Runeterra",
}

def _varint(data, idx):
    r, s = 0, 0
    while idx < len(data):
        b = data[idx]; idx += 1
        r |= (b & 0x7F) << s
        if not (b & 0x80): break
        s += 7
    return r, idx

def decode(code: str) -> list[dict]:
    """Decodifica un deck_code → lista de {card_code, count, faction, set}."""
    bits = "".join(format(B32.index(c), "05b") for c in code.upper())
    raw  = [int(bits[i:i+8], 2) for i in range(0, (len(bits)//8)*8, 8)]
    cards, idx = [], 1

    for count in (3, 2):
        if idx >= len(raw): break
        ng, idx = _varint(raw, idx)
        for _ in range(ng):
            if idx >= len(raw): break
            nc, idx = _varint(raw, idx)
            if idx >= len(raw): break
            s,  idx = _varint(raw, idx)
            if idx >= len(raw): break
            f,  idx = _varint(raw, idx)
            fc = FACTION_ID.get(f, f"F{f:02d}")
            for _ in range(nc):
                if idx >= len(raw): break
                n, idx = _varint(raw, idx)
                cards.append({"card_code": f"{s:02d}{fc}{n:03d}",
                              "count": count, "set": s,
                              "faction": FACTION_NAME.get(fc, fc)})

    if idx < len(raw):
        ns, idx = _varint(raw, idx)
        for _ in range(ns):
            if idx >= len(raw): break
            s,  idx = _varint(raw, idx)
            f,  idx = _varint(raw, idx)
            n,  idx = _varint(raw, idx)
            fc = FACTION_ID.get(f, f"F{f:02d}")
            cards.append({"card_code": f"{s:02d}{fc}{n:03d}",
                          "count": 1, "set": s,
                          "faction": FACTION_NAME.get(fc, fc)})
    return cards


def deck_to_row_fields(deck_code: str) -> dict:
    """
    Convierte un deck_code en los campos que irán al dataset:
      - factions        : facciones presentes (string separado por |)
      - total_cards     : copias totales
      - unique_cards    : cartas únicas
      - card_list       : JSON [{card_code, count, faction}]
      - mana_low_pct    : % cartas coste 1-2  (proxy sin Data Dragon)
      - mana_mid_pct    : % cartas coste 3-4
      - mana_high_pct   : % cartas coste 5+
      - archetype_hint  : aggro / midrange / control  (heurístico por card_number)
    """
    try:
        cards = decode(deck_code)
    except Exception:
        return {"factions": None, "total_cards": None, "unique_cards": None,
                "card_list": None, "mana_low_pct": None,
                "mana_mid_pct": None, "mana_high_pct": None,
                "archetype_hint": None}

    total  = sum(c["count"] for c in cards)
    facs   = sorted({c["faction"] for c in cards})

    # Proxy de curva de maná basado en card_number
    # (sin Data Dragon el coste real no está disponible)
    # card_number <= 20  → cartas tempranas (bajo coste estadísticamente)
    # card_number 21-35  → medio coste
    # card_number > 35   → alto coste
    low  = sum(c["count"] for c in cards if c["card_code"][-3:].lstrip("0") != ""
               and int(c["card_code"][-3:]) <= 20)
    mid  = sum(c["count"] for c in cards if c["card_code"][-3:].lstrip("0") != ""
               and 21 <= int(c["card_code"][-3:]) <= 35)
    high = sum(c["count"] for c in cards if c["card_code"][-3:].lstrip("0") != ""
               and int(c["card_code"][-3:]) > 35)

    if total > 0:
        lp = round(low  / total * 100, 1)
        mp = round(mid  / total * 100, 1)
        hp = round(high / total * 100, 1)
        arch = "aggro" if lp > 50 else "control" if hp > 30 else "midrange"
    else:
        lp = mp = hp = None
        arch = None

    return {
        "factions":       "|".join(facs),
        "total_cards":    total,
        "unique_cards":   len(cards),
        "card_list":      json.dumps([{"c": c["card_code"], "n": c["count"]}
                                       for c in cards], separators=(",", ":")),
        "mana_low_pct":   lp,
        "mana_mid_pct":   mp,
        "mana_high_pct":  hp,
        "archetype_hint": arch,
    }


# ══════════════════════════════════════════════════════════════
# CLIENTE DE LA API  (con rate-limit y penalización controlada)
# ══════════════════════════════════════════════════════════════

BASE_URL = f"https://{REGION}.api.riotgames.com"
HEADERS  = {"X-Riot-Token": API_KEY}


class RateLimitPenalty(Exception):
    def __init__(self, wait): self.wait = wait


def get(endpoint, params=None, retries=3):
    url = f"{BASE_URL}{endpoint}"
    for _ in range(retries):
        r = requests.get(url, headers=HEADERS, params=params)
        if r.status_code == 200:
            return r.json()
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 10))
            if wait > MAX_PENALTY:
                raise RateLimitPenalty(wait)
            print(f"    ⏳ Rate limit — esperando {wait}s...")
            time.sleep(wait)
            continue
        if r.status_code == 404:
            return None
        if r.status_code == 401:
            print("    ✗ 401 — API Key expirada. Regénérala en developer.riotgames.com")
            raise SystemExit(1)
        return None
    return None


# ══════════════════════════════════════════════════════════════
# CHECKPOINT
# ══════════════════════════════════════════════════════════════

def load_checkpoint() -> dict:
    if CHECKPOINT.exists():
        state = json.loads(CHECKPOINT.read_text())
        print(f"  ✓ Checkpoint: {state['done']} jugadores procesados previamente.")
        return state
    return {"done": 0, "processed_names": [], "failed_names": []}


def save_checkpoint(state: dict):
    CHECKPOINT.write_text(json.dumps(state, indent=2))


# ══════════════════════════════════════════════════════════════
# GUARDADO INCREMENTAL EN CSV + XLSX
# ══════════════════════════════════════════════════════════════

COLUMNS = [
    "extraction_date", "rank", "player_name", "lp",
    "puuid", "deck_code",
    "factions", "total_cards", "unique_cards",
    "mana_low_pct", "mana_mid_pct", "mana_high_pct",
    "archetype_hint", "card_list",
]


def load_existing() -> pd.DataFrame:
    """Carga el CSV existente o crea un DataFrame vacío."""
    if OUTPUT_CSV.exists():
        df = pd.read_csv(OUTPUT_CSV, dtype=str)
        print(f"  ✓ Dataset existente: {len(df)} filas.")
        return df
    return pd.DataFrame(columns=COLUMNS)


def append_row(df: pd.DataFrame, row: dict) -> pd.DataFrame:
    """Añade una fila al DataFrame (sin duplicar mismo player+fecha)."""
    new = pd.DataFrame([row], columns=COLUMNS)
    return pd.concat([df, new], ignore_index=True)


def save_outputs(df: pd.DataFrame):
    """Guarda CSV y XLSX sobreescribiendo."""
    df.to_csv(OUTPUT_CSV, index=False)

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="LOR_Masters")

        ws = writer.sheets["LOR_Masters"]

        # Ancho de columnas automático
        for col in ws.columns:
            max_len = max(
                len(str(cell.value)) if cell.value else 0
                for cell in col
            )
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 60)

        # Fila de encabezado en negrita
        from openpyxl.styles import Font, PatternFill, Alignment
        header_fill = PatternFill("solid", fgColor="1F4E79")
        for cell in ws[1]:
            cell.font      = Font(bold=True, color="FFFFFF", name="Arial", size=10)
            cell.fill      = header_fill
            cell.alignment = Alignment(horizontal="center")

        # Filas de datos
        for row in ws.iter_rows(min_row=2):
            for cell in row:
                cell.font      = Font(name="Arial", size=10)
                cell.alignment = Alignment(horizontal="left")

        # Freeze header
        ws.freeze_panes = "A2"


# ══════════════════════════════════════════════════════════════
# LÓGICA DEL PIPELINE POR JUGADOR
# ══════════════════════════════════════════════════════════════

def resolve_puuid(name: str) -> str | None:
    for tag in CANDIDATE_TAGS:
        data = get(f"/riot/account/v1/accounts/by-riot-id/{name}/{tag}")
        time.sleep(DELAY)
        if data and "puuid" in data:
            return data["puuid"]
    return None


def most_used_deck(puuid: str) -> str | None:
    """Obtiene el deck_code más usado en las últimas N partidas del jugador."""
    ids = get(f"/lor/match/v1/matches/by-puuid/{puuid}/ids",
              params={"count": MATCHES_COUNT})
    time.sleep(DELAY)
    if not isinstance(ids, list) or not ids:
        return None

    counts = Counter()
    for mid in ids:
        match = get(f"/lor/match/v1/matches/{mid}")
        time.sleep(DELAY)
        if not match:
            continue
        for p in match.get("info", {}).get("players", []):
            if p.get("puuid") == puuid:
                code = p.get("deck_code")
                if code:
                    counts[code] += 1
                break

    if not counts:
        return None
    return counts.most_common(1)[0][0]


# ══════════════════════════════════════════════════════════════
# EJECUCIÓN PRINCIPAL
# ══════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("  LOR Extractor — Top 1750 Maestros")
    print("=" * 60)
    print(f"  Región     : {REGION}")
    print(f"  Jugadores  : {TOP_N}")
    print(f"  Partidas   : últimas {MATCHES_COUNT} por jugador")
    print(f"  Delay      : {DELAY}s  |  Penalización máx: {MAX_PENALTY}s")
    est_hours = (TOP_N * (1 + MATCHES_COUNT) * DELAY) / 3600
    print(f"  Tiempo est.: ~{est_hours:.1f} horas (primera extracción completa)")
    print(f"  Outputs    : {OUTPUT_CSV}  +  {OUTPUT_XLSX}\n")

    # Cargar estado
    state  = load_checkpoint()
    df     = load_existing()
    today  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    done_names = set(state["processed_names"])

    # Leaderboard
    print("[1/4] Obteniendo leaderboard Maestro...")
    lb = get("/lor/ranked/v1/leaderboards")
    if not lb or "players" not in lb:
        print("  ✗ No se pudo obtener el leaderboard. Verifica la API Key.")
        return
    players = lb["players"][:TOP_N]
    print(f"  ✓ {len(players)} jugadores en el leaderboard.\n")

    print(f"[2-4/4] Procesando jugadores...")
    saved_count = 0

    for i, player in enumerate(players, 1):
        name = player.get("name", "")
        lp   = player.get("lp", 0)

        # Saltar si ya se procesó HOY (permite re-extracción en días distintos)
        already_today = (
            not df.empty and
            ((df["player_name"] == name) & (df["extraction_date"] == today)).any()
        )
        if already_today:
            print(f"  [{i:>4}/{len(players)}] {name:<28} — ya extraído hoy, omitiendo.")
            continue

        print(f"  [{i:>4}/{len(players)}] {name:<28}  LP: {lp:<6}", end="", flush=True)

        try:
            # PUUID
            puuid = resolve_puuid(name)
            if not puuid:
                print(f"  ✗ sin PUUID")
                state["failed_names"].append(name)
                continue

            # Mazo más usado
            deck_code = most_used_deck(puuid)
            if not deck_code:
                print(f"  ✗ sin partidas")
                continue

            # Decodificar
            fields = deck_to_row_fields(deck_code)

            # Construir fila
            row = {
                "extraction_date": today,
                "rank":            i,
                "player_name":     name,
                "lp":              lp,
                "puuid":           puuid,
                "deck_code":       deck_code,
                **fields,
            }

            # Añadir al DataFrame y guardar
            df = append_row(df, row)
            save_outputs(df)
            saved_count += 1

            arch = fields.get("archetype_hint", "?")
            facs = (fields.get("factions") or "?")[:30]
            print(f"  ✓  {arch:<9}  {facs}")

            # Checkpoint cada 25 jugadores
            state["processed_names"].append(name)
            if i % 25 == 0:
                state["done"] = i
                save_checkpoint(state)
                print(f"\n  💾 Checkpoint guardado — {i}/{len(players)} jugadores, "
                      f"{len(df)} filas totales en dataset.\n")

        except RateLimitPenalty as e:
            state["done"] = i
            save_checkpoint(state)
            save_outputs(df)
            print(f"\n  🛑 Penalización larga detectada ({e.wait}s ≈ {e.wait//60}min).")
            print(f"     Dataset guardado: {len(df)} filas.")
            print(f"     Espera ~{e.wait//60} minutos y vuelve a ejecutar.")
            print(f"     El script retomará automáticamente desde el jugador {i}.")
            return

    # Guardado final
    state["done"] = len(players)
    save_checkpoint(state)
    save_outputs(df)

    print(f"\n{'='*60}")
    print(f"  ✅ Extracción completada")
    print(f"     Filas nuevas esta ejecución : {saved_count}")
    print(f"     Total filas en el dataset   : {len(df)}")
    print(f"     Archivos: {OUTPUT_CSV}  |  {OUTPUT_XLSX}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
