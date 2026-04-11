"""
=============================================================
  LOR — Vectorización de mazos + Reducción de dimensionalidad
=============================================================
  Entrada : lor_dataset.xlsx
  Salidas :
    lor_vectors.csv        — matriz de features completa
    lor_pca_2d.csv         — coordenadas PCA 2D
    lor_tsne_2d.csv        — coordenadas t-SNE 2D
    lor_pca_variance.csv   — varianza explicada por componente

  Pipeline de features:
    BLOQUE A — Presencia/cantidad de cartas  (196 dims)
               Cada carta del pool es una columna.
               Valor = número de copias (0, 1, 2 o 3).

    BLOQUE B — Curva de maná real            (8 dims)
               Descargada de Data Dragon.
               Bins: coste 0, 1, 2, 3, 4, 5, 6, 7+
               Valor = copias normalizadas por total del mazo.
               Si Data Dragon no disponible → columnas en 0.

    BLOQUE C — Facciones one-hot             (16 dims)
               Una columna por facción conocida + desconocidas.
               Valor = 1 si el mazo incluye esa facción.

  Reducción de dimensionalidad:
    · PCA  → retiene componentes que explican el 95% de varianza
    · PCA  → versión 2D para visualización
    · t-SNE → 2D para visualización de clusters

  Uso:
    python lor_vectorizer.py

  Requisitos:
    pip install pandas numpy scikit-learn openpyxl requests
=============================================================
"""

import json
import requests
import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE


# ══════════════════════════════════════════════════════════════
# CONFIGURACIÓN
# ══════════════════════════════════════════════════════════════

INPUT_FILE   = Path("lor_dataset.xlsx")
OUT_VECTORS  = Path("lor_vectors.csv")
OUT_PCA_2D   = Path("lor_pca_2d.csv")
OUT_TSNE_2D  = Path("lor_tsne_2d.csv")
OUT_VARIANCE = Path("lor_pca_variance.csv")

# Sets de Data Dragon a pre-cargar (incluye Eternal)
DD_SETS = list(range(1, 11)) + [19, 20, 22, 24, 38, 41, 52, 56]

# Facciones conocidas (orden fijo para el vector)
KNOWN_FACTIONS = [
    "Demacia", "Freljord", "Ionia", "Noxus",
    "Piltover & Zaun", "Shadow Isles", "Bilgewater",
    "Shurima", "Mount Targon", "Bandle City", "Runeterra",
]

# Varianza mínima a retener en el PCA completo
PCA_VARIANCE_THRESHOLD = 0.95

# t-SNE: semilla para reproducibilidad
TSNE_RANDOM_STATE = 42


# ══════════════════════════════════════════════════════════════
# DATA DRAGON — caché de costes de maná
# ══════════════════════════════════════════════════════════════

_DD_CACHE: dict[int, dict] = {}


def load_set(set_id: int) -> None:
    if set_id in _DD_CACHE:
        return
    url = (f"https://dd.b.pvp.net/latest/set{set_id}"
           f"/en_us/data/set{set_id}-en_us.json")
    try:
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            data = r.json()
            if data:
                _DD_CACHE[set_id] = {c["cardCode"]: c for c in data}
                print(f"  [DD] Set {set_id:>2}: {len(_DD_CACHE[set_id])} cartas")
                return
        _DD_CACHE[set_id] = {}
        print(f"  [DD] Set {set_id:>2}: HTTP {r.status_code} — sin datos")
    except Exception as e:
        _DD_CACHE[set_id] = {}
        print(f"  [DD] Set {set_id:>2}: {e}")


def get_cost(card_code: str) -> int | None:
    set_id = int(card_code[:2])
    if set_id not in _DD_CACHE:
        load_set(set_id)
    meta = _DD_CACHE.get(set_id, {}).get(card_code)
    return meta.get("cost") if meta else None


def preload_dd() -> None:
    print("[1/5] Pre-cargando Data Dragon...")
    for s in DD_SETS:
        load_set(s)
    total = sum(len(v) for v in _DD_CACHE.values())
    print(f"  ✓ {total} cartas en caché\n")


# ══════════════════════════════════════════════════════════════
# CARGA Y PARSING DEL DATASET
# ══════════════════════════════════════════════════════════════

def load_dataset() -> pd.DataFrame:
    print("[2/5] Cargando dataset...")
    df = pd.read_excel(INPUT_FILE)
    df["card_list_parsed"] = df["card_list"].apply(json.loads)
    print(f"  ✓ {len(df)} mazos cargados")
    print(f"  ✓ Columnas: {df.columns.tolist()}\n")
    return df


# ══════════════════════════════════════════════════════════════
# CONSTRUCCIÓN DEL VECTOR DE FEATURES
# ══════════════════════════════════════════════════════════════

def build_card_universe(df: pd.DataFrame) -> list[str]:
    """Universo ordenado de todas las cartas presentes en el dataset."""
    cards = set()
    for card_list in df["card_list_parsed"]:
        for c in card_list:
            cards.add(c["c"])
    return sorted(cards)


def faction_columns(df: pd.DataFrame) -> list[str]:
    """Lista de todas las facciones presentes, con las conocidas primero."""
    unknown = set()
    for facs in df["factions"]:
        for f in facs.split("|"):
            f = f.strip()
            if f not in KNOWN_FACTIONS:
                unknown.add(f)
    return KNOWN_FACTIONS + sorted(unknown)


def build_mana_curve(card_list: list[dict]) -> np.ndarray:
    """
    Vector de 8 posiciones: proporción de copias por bin de coste (0-7+).
    Normalizado por total de copias con coste conocido.
    Si ninguna carta tiene coste disponible → vector de ceros.
    """
    bins   = np.zeros(8)   # índices 0-7 (7 = 7+)
    total  = 0
    for c in card_list:
        cost = get_cost(c["c"])
        if cost is None:
            continue
        idx = min(cost, 7)
        bins[idx] += c["n"]
        total     += c["n"]
    return bins / total if total > 0 else bins


def vectorize(df: pd.DataFrame,
              card_universe: list[str],
              all_factions: list[str]) -> pd.DataFrame:
    """
    Construye la matriz de features con tres bloques:
      A: copias de cada carta  (len(card_universe) dims)
      B: curva de maná         (8 dims)
      C: facciones one-hot     (len(all_factions) dims)
    """
    # Índices para lookup rápido
    card_idx    = {c: i for i, c in enumerate(card_universe)}
    faction_idx = {f: i for i, f in enumerate(all_factions)}

    n_cards    = len(card_universe)
    n_mana     = 8
    n_factions = len(all_factions)
    n_features = n_cards + n_mana + n_factions

    matrix = np.zeros((len(df), n_features), dtype=np.float32)

    mana_dd_ok = 0   # contador de mazos con curva de maná completa

    for row_i, (_, row) in enumerate(df.iterrows()):
        card_list = row["card_list_parsed"]

        # ── Bloque A: copias de cartas ────────────────────────────────────────
        for c in card_list:
            col = card_idx.get(c["c"])
            if col is not None:
                matrix[row_i, col] = c["n"]

        # ── Bloque B: curva de maná ───────────────────────────────────────────
        mana = build_mana_curve(card_list)
        start_b = n_cards
        matrix[row_i, start_b : start_b + n_mana] = mana
        if mana.sum() > 0:
            mana_dd_ok += 1

        # ── Bloque C: facciones one-hot ───────────────────────────────────────
        start_c = n_cards + n_mana
        for fac in row["factions"].split("|"):
            fac = fac.strip()
            col = faction_idx.get(fac)
            if col is not None:
                matrix[row_i, start_c + col] = 1.0

    # Nombres de columnas
    mana_cols    = [f"mana_{i}" for i in range(7)] + ["mana_7plus"]
    faction_cols = [f"fac_{f.replace(' ', '_').replace('&', 'and')}"
                    for f in all_factions]
    col_names    = (
        [f"card_{c}" for c in card_universe] +
        mana_cols +
        faction_cols
    )

    print(f"  ✓ Bloque A (cartas)   : {n_cards} dims")
    print(f"  ✓ Bloque B (maná)     : {n_mana} dims "
          f"[{mana_dd_ok}/{len(df)} mazos con DD]")
    print(f"  ✓ Bloque C (facciones): {n_factions} dims")
    print(f"  ✓ Total features      : {n_features} dims")

    df_out = pd.DataFrame(matrix, columns=col_names)
    df_out.insert(0, "player_name", df["player_name"].values)
    df_out.insert(1, "rank",        df["rank"].values)
    df_out.insert(2, "factions",    df["factions"].values)
    return df_out


# ══════════════════════════════════════════════════════════════
# REDUCCIÓN DE DIMENSIONALIDAD
# ══════════════════════════════════════════════════════════════

def run_pca(X_scaled: np.ndarray,
            meta: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    PCA doble:
      1. n_components que explican >= PCA_VARIANCE_THRESHOLD de varianza
      2. 2 componentes para visualización
    Retorna (df_variance, df_2d)
    """
    # ── PCA completo para análisis de varianza ────────────────────────────────
    n_max = min(len(X_scaled), X_scaled.shape[1])
    pca_full = PCA(n_components=n_max, random_state=42)
    pca_full.fit(X_scaled)

    cumvar = np.cumsum(pca_full.explained_variance_ratio_)
    n_95   = int(np.searchsorted(cumvar, PCA_VARIANCE_THRESHOLD)) + 1

    df_variance = pd.DataFrame({
        "component":          range(1, n_max + 1),
        "explained_variance": pca_full.explained_variance_ratio_,
        "cumulative_variance": cumvar,
    })

    print(f"  ✓ Componentes para {PCA_VARIANCE_THRESHOLD*100:.0f}% varianza: {n_95}")
    print(f"  ✓ Varianza PC1: {pca_full.explained_variance_ratio_[0]*100:.1f}%  "
          f"PC2: {pca_full.explained_variance_ratio_[1]*100:.1f}%")

    # ── PCA 2D para visualización ─────────────────────────────────────────────
    pca_2d = PCA(n_components=2, random_state=42)
    coords = pca_2d.fit_transform(X_scaled)

    df_2d = meta.copy()
    df_2d["PC1"] = coords[:, 0]
    df_2d["PC2"] = coords[:, 1]

    return df_variance, df_2d


def run_tsne(X_scaled: np.ndarray,
             meta: pd.DataFrame) -> pd.DataFrame:
    """
    t-SNE 2D. Perplexity se ajusta automáticamente si n < 30.
    """
    n = len(X_scaled)
    perplexity = min(5, n - 1)   # t-SNE requiere perplexity < n

    tsne = TSNE(
        n_components=2,
        perplexity=perplexity,
        random_state=TSNE_RANDOM_STATE,
        max_iter=1000,
        init="pca",
    )
    coords = tsne.fit_transform(X_scaled)

    df_tsne = meta.copy()
    df_tsne["tSNE1"] = coords[:, 0]
    df_tsne["tSNE2"] = coords[:, 1]

    print(f"  ✓ t-SNE perplexity usada: {perplexity} (n={n})")
    return df_tsne


# ══════════════════════════════════════════════════════════════
# EJECUCIÓN PRINCIPAL
# ══════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("  LOR Vectorizer — Construcción de features + PCA/t-SNE")
    print("=" * 60)

    # ── 1. Data Dragon ────────────────────────────────────────────────────────
    preload_dd()

    # ── 2. Dataset ────────────────────────────────────────────────────────────
    df = load_dataset()

    # Advertencia sobre mazos incompletos
    incomplete = df[df["total_cards"] < 40]
    if len(incomplete) > 0:
        print(f"  ⚠  {len(incomplete)} mazo(s) con < 40 cartas "
              f"(se incluyen en el análisis):\n"
              f"     {incomplete['player_name'].tolist()}\n")

    # ── 3. Vectorización ──────────────────────────────────────────────────────
    print("[3/5] Construyendo vectores de features...")
    card_universe = build_card_universe(df)
    all_factions  = faction_columns(df)
    df_vectors    = vectorize(df, card_universe, all_factions)

    df_vectors.to_csv(OUT_VECTORS, index=False)
    print(f"  ✓ Guardado: {OUT_VECTORS}\n")

    # ── 4. Escalado ───────────────────────────────────────────────────────────
    print("[4/5] Escalando features (StandardScaler)...")
    feature_cols = [c for c in df_vectors.columns
                    if c not in ("player_name", "rank", "factions")]
    X = df_vectors[feature_cols].values.astype(np.float32)

    scaler  = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    meta_cols = df_vectors[["player_name", "rank", "factions"]].copy()

    # Añadir archetype_hint al meta si está disponible
    if "archetype_hint" in df.columns:
        meta_cols["archetype_hint"] = df["archetype_hint"].values

    print(f"  ✓ Matriz escalada: {X_scaled.shape[0]} mazos × "
          f"{X_scaled.shape[1]} features\n")

    # ── 5. Reducción de dimensionalidad ───────────────────────────────────────
    print("[5/5] Reducción de dimensionalidad...")

    print("  · PCA...")
    df_variance, df_pca_2d = run_pca(X_scaled, meta_cols)
    df_variance.to_csv(OUT_VARIANCE, index=False)
    df_pca_2d.to_csv(OUT_PCA_2D, index=False)
    print(f"  ✓ Guardado: {OUT_VARIANCE}")
    print(f"  ✓ Guardado: {OUT_PCA_2D}")

    print("\n  · t-SNE...")
    df_tsne = run_tsne(X_scaled, meta_cols)
    df_tsne.to_csv(OUT_TSNE_2D, index=False)
    print(f"  ✓ Guardado: {OUT_TSNE_2D}")

    # ── Resumen final ─────────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print(f"  ✅ Pipeline completado")
    print(f"     Mazos procesados : {len(df)}")
    print(f"     Features totales : {len(feature_cols)}")
    print(f"       · Cartas únicas: {len(card_universe)}")
    print(f"       · Bins de maná : 8")
    print(f"       · Facciones    : {len(all_factions)}")
    print(f"\n  Archivos generados:")
    print(f"     {OUT_VECTORS}   — matriz completa de features")
    print(f"     {OUT_PCA_2D}     — coordenadas PCA 2D")
    print(f"     {OUT_TSNE_2D}    — coordenadas t-SNE 2D")
    print(f"     {OUT_VARIANCE}   — varianza explicada por componente PCA")
    print(f"\n  ⚠  Nota: con {len(df)} mazos el dataset es aún pequeño.")
    print(f"     A medida que acumules más extracciones periódicas,")
    print(f"     el clustering y la PCA ganarán robustez estadística.")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
