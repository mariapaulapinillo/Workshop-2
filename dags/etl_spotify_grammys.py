import os
from datetime import datetime
import re, sys, subprocess, unicodedata
from typing import Tuple
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError
from airflow.decorators import dag, task

# =========================
# Rutas dentro del contenedor
# =========================
DATA_DIR = os.getenv("AIRFLOW_DATA_DIR", "/opt/airflow/data")
STAGING_DIR = os.path.join(DATA_DIR, "staging")
OUTPUT_DIR = os.path.join(DATA_DIR, "output")
os.makedirs(STAGING_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# =========================
# Conexión a Postgres (sin Hook)
# =========================
PG_URL = os.getenv(
    "PG_EXTRACT_URL",
    "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
)

def get_engine():
    # pool_pre_ping evita conexiones muertas
    return create_engine(PG_URL, pool_pre_ping=True)

# =====================================================
# Helpers de normalización, manejo de nulos y fuzzy
# =====================================================
def _strip_accents(s: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))

def normalize_text(s: str) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    s = str(s).lower().strip()
    s = _strip_accents(s)
    # unificar conectores comunes
    s = re.sub(r"\b(feat|featuring|ft)\.?\b", "feat", s)
    s = s.replace("&", " and ")
    s = re.sub(r"[^\w\s]", " ", s)   # quitar puntuación
    s = re.sub(r"\s+", " ", s).strip()
    return s

def split_genre(g: str) -> Tuple[str, str]:
    g = normalize_text(g)
    if not g:
        return "unknown", None
    for sep in (" / ", "/", " - ", "-", " | ", "|", " > ", ">"):
        if sep in g:
            parts = [p.strip() for p in g.split(sep) if p.strip()]
            return (parts[0] if parts else "unknown",
                    parts[1] if len(parts) > 1 else None)
    return (g, None)

# intentar importar RapidFuzz; si no existe, instalarlo al vuelo
try:
    from rapidfuzz import process, fuzz
except ModuleNotFoundError:
    print("⚙️ Instalando rapidfuzz compatible con Python 3.7...")
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "rapidfuzz==2.13.7"],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    from rapidfuzz import process, fuzz

def sanitize_spotify(sp: pd.DataFrame) -> pd.DataFrame:
    """Limpieza + nulos Spotify"""
    sp = sp.copy()
    keep = [
        "track_id", "track_name", "artists", "artist", "popularity",
        "danceability", "energy", "loudness", "tempo", "track_genre", "release_date"
    ]
    sp = sp[[c for c in sp.columns if c in keep]]

    # si no hay 'artist', derivarlo de 'artists'
    if "artist" not in sp.columns and "artists" in sp.columns:
        sp["artist"] = sp["artists"].astype(str).str.split(",").str[0].str.strip()

    # rellenar vacíos de texto (para normalizar y matchear)
    for c in ["track_name", "artists", "artist", "track_genre", "release_date"]:
        if c in sp.columns:
            sp[c] = sp[c].fillna("")

    # eliminar duplicados y filas sin información mínima para matching
    sp = sp.drop_duplicates(subset=["track_id"])
    sp = sp[~(sp["artist"].str.strip().eq("")) | ~(sp["track_name"].str.strip().eq(""))]

    # género jerárquico
    sp["main_genre"], sp["sub_genre"] = zip(
        *sp.get("track_genre", pd.Series([""] * len(sp))).map(split_genre)
    )

    # campos limpios para matching
    sp["artist_clean"] = sp["artist"].map(normalize_text)
    sp["title_clean"] = sp.get("track_name", pd.Series([""] * len(sp))).map(normalize_text)

    return sp.reset_index(drop=True)

def sanitize_grammys(gr: pd.DataFrame) -> pd.DataFrame:
    """Limpieza + nulos Grammys"""
    gr = gr.copy()
    keep = ["year", "category", "nominee", "artist", "winner", "title"]
    gr = gr[[c for c in gr.columns if c in keep]]

    # tipados
    if "year" in gr.columns:
        gr["year"] = pd.to_numeric(gr["year"], errors="coerce").astype("Int64")
    if "winner" in gr.columns:
        gr["winner"] = (
            gr["winner"].astype(str)
            .str.strip().str.lower()
            .map({"true": True, "1": True, "false": False, "0": False})
        )

    # rellenar texto
    for c in ["artist", "nominee", "title", "category"]:
        if c in gr.columns:
            gr[c] = gr[c].fillna("")

    # decidir columna título
    title_col = "nominee" if "nominee" in gr.columns else ("title" if "title" in gr.columns else None)
    if title_col is None:
        gr["nominee"] = ""
        title_col = "nominee"

    # limpiar
    gr["artist_clean"] = gr["artist"].map(normalize_text) if "artist" in gr.columns else ""
    gr["title_clean"] = gr[title_col].map(normalize_text)

    # descartar registros sin info mínima
    gr = gr[~gr["artist_clean"].eq("") & ~gr["title_clean"].eq("")]
    gr = gr.drop_duplicates().reset_index(drop=True)
    return gr

# =========================
# DAG
# =========================
@dag(
    dag_id="etl_spotify_grammys",
    start_date=datetime(2025, 10, 1),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "spotify", "grammys"],
)
def etl_spotify_grammys():

    @task()
    def extract_spotify_csv(path: str = os.path.join(DATA_DIR, "spotify_dataset.csv")) -> str:
        # sep=None + engine="python" autodetecta separador
        df = pd.read_csv(path, sep=None, engine="python", encoding="utf-8-sig")
        out_path = os.path.join(STAGING_DIR, "spotify_extracted.parquet")
        df.to_parquet(out_path, index=False)
        print(f"Spotify OK: {df.shape} -> {out_path}")
        return out_path

    @task()
    def extract_grammys_db() -> str:
        engine = get_engine()
        csv_path = os.path.join(DATA_DIR, "the_grammy_awards.csv")
        out_path = os.path.join(STAGING_DIR, "grammys_extracted.parquet")

        # Asegura schema
        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))

        try:
            # 1) Intentar leer la tabla
            df = pd.read_sql("SELECT * FROM raw.grammys;", con=engine)
            print(f"Grammys (DB) OK: {df.shape}")
        except ProgrammingError:
            # 2) Si no existe, cargar desde CSV -> DB -> leer
            print("Tabla raw.grammys no existe. Cargando desde CSV…")
            df_csv = pd.read_csv(csv_path, encoding="utf-8-sig")
            if "winner" in df_csv.columns:
                df_csv["winner"] = (
                    df_csv["winner"].astype(str).str.strip().str.lower()
                    .map({"true": True, "1": True, "false": False, "0": False})
                )
            df_csv.to_sql("grammys", engine, schema="raw", if_exists="replace", index=False)
            df = pd.read_sql("SELECT * FROM raw.grammys;", con=engine)
            print(f"Grammys cargado y leído: {df.shape}")

        df.to_parquet(out_path, index=False)
        print(f"Grammys OK -> {out_path}")
        return out_path

    @task()
    def transform_merge(spotify_parquet: str, grammys_parquet: str) -> str:
        # --- Cargar ---
        sp_raw = pd.read_parquet(spotify_parquet)
        gr_raw = pd.read_parquet(grammys_parquet)

        # --- Limpiar y manejar nulos ---
        sp = sanitize_spotify(sp_raw)
        gr = sanitize_grammys(gr_raw)

        # --- Fuzzy match por artista y título con threshold 90 ---
        sp_artists = sp["artist_clean"].astype(str).tolist()
        matches = []

        for i, row in gr.iterrows():
            a_clean, t_clean = row["artist_clean"], row["title_clean"]
            # shortlist por artista
            artist_candidates = process.extract(
                a_clean, sp_artists, scorer=fuzz.token_sort_ratio, limit=5
            )
            best = None
            best_score = -1
            for cand in artist_candidates:
                sp_index, score_a = cand[2], cand[1]
                if score_a < 90:
                    continue
                score_t = fuzz.token_set_ratio(t_clean, sp.at[sp_index, "title_clean"])
                if score_t >= 90:
                    avg = (score_a + score_t) / 2
                    if avg > best_score:
                        best = (sp_index, i, score_a, score_t, avg)
                        best_score = avg
            if best is not None:
                matches.append(best)

        m = (
            pd.DataFrame(matches, columns=["sp_idx", "gr_idx", "score_artist", "score_title", "score_avg"])
            if matches
            else pd.DataFrame(columns=["sp_idx", "gr_idx", "score_artist", "score_title", "score_avg"])
        )

        # --- Bandera y enriquecimiento ---
        sp["grammy_nominee"] = False
        if not m.empty:
            sp.loc[m["sp_idx"].unique(), "grammy_nominee"] = True

        gram_extra_cols = [c for c in ["year", "category", "winner"] if c in gr.columns]
        if gram_extra_cols and not m.empty:
            # traer metadatos de Grammys a las filas de Spotify matcheadas
            enrich = m.merge(
                gr[gram_extra_cols].reset_index().rename(columns={"index": "gr_row"}),
                left_on="gr_idx",
                right_on="gr_row",
                how="left",
            )
            for c in gram_extra_cols:
                if c not in sp.columns:
                    sp[c] = pd.NA
                sp.loc[enrich["sp_idx"], c] = enrich[c].values

        # --- salida ---
        out_path = os.path.join(STAGING_DIR, "merged.parquet")
        sp.to_parquet(out_path, index=False)
        print(f"Merged (fuzzy + nulos manejados): {sp.shape} -> {out_path}")
        print(f"Total matches: {len(matches)} / {len(gr)} ({(len(matches)/max(1,len(gr))):.1%})")
        # reporte rápido de nulos en columnas clave
        key_cols = [c for c in ["artist", "track_name", "main_genre", "year", "category", "winner", "grammy_nominee"] if c in sp.columns]
        if key_cols:
            print("Nulls % claves:\n", (sp[key_cols].isna().mean() * 100).round(2).sort_values(ascending=False).to_string())
        return out_path

    @task()
    def load_to_gdrive_csv(merged_parquet: str) -> str:
        df = pd.read_parquet(merged_parquet)
        out_csv = os.path.join(OUTPUT_DIR, "merged_spotify_grammys.csv")
        df.to_csv(out_csv, index=False, encoding="utf-8-sig")
        print(f"CSV listo: {out_csv} ({df.shape})")
        return out_csv

    @task()
    def load_to_dw(merged_parquet: str) -> str:
        df = pd.read_parquet(merged_parquet)
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS dw;"))
        df.to_sql("spotify_grammys", engine, schema="dw", if_exists="replace", index=False)
        print(f"Cargado DW: dw.spotify_grammys ({df.shape})")
        return "dw.spotify_grammys"

    # ==== Orquestación ====
    sp_path = extract_spotify_csv()
    gr_path = extract_grammys_db()
    merged_path = transform_merge(sp_path, gr_path)
    _csv = load_to_gdrive_csv(merged_path)
    _dw = load_to_dw(merged_path)

etl_spotify_grammys()
