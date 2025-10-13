# %%
# ===============================================================
# VISUALIZACIONES Y KPIs — Spotify + Grammys (versión simple & clara)
# ===============================================================

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# ---------- CARGA ----------
CSV_CANDIDATES = [
    "data/output/merged_spotify_grammys.csv",
    "data/spotify_grammy_final.csv",
    "../data/spotify_grammy_final.csv",
]

CSV_FILE = next((p for p in CSV_CANDIDATES if os.path.exists(p)), None)
if not CSV_FILE:
    raise FileNotFoundError(f"No encontré CSV. Probé:\n- " + "\n- ".join(CSV_CANDIDATES))

df = pd.read_csv(CSV_FILE)
print(f"✔ Cargado: {CSV_FILE} ({len(df):,} filas)")

# ---------- ESTILO ----------
sns.set_theme(style="whitegrid")
plt.rcParams["axes.titlesize"] = 12
plt.rcParams["axes.labelsize"] = 11

# ---------- LIMPIEZA BÁSICA ----------
need_cols = [
    "artist","nominee","match_type","track_genre","popularity","year",
    "explicit","winner","grammy_nominee","danceability","energy","loudness","tempo"
]
for c in need_cols:
    if c not in df.columns:
        df[c] = np.nan

df["year"] = pd.to_numeric(df["year"], errors="coerce")
df["popularity"] = pd.to_numeric(df["popularity"], errors="coerce")
df["explicit"] = df["explicit"].astype(str).str.lower().map(
    {"true": True, "1": True, "false": False, "0": False}
)
df["winner"] = df["winner"].astype(str).str.lower().map(
    {"true": True, "1": True, "false": False, "0": False}
)

# ---------- ARTISTA PRINCIPAL (como tu guía) ----------
def main_artist(row):
    if str(row.get("match_type","")) == "artist" and pd.notna(row.get("nominee")):
        return str(row["nominee"]).strip()
    a = str(row.get("artist","")).split(",")[0].strip()
    return a if a else np.nan

df["artist_main"] = df.apply(main_artist, axis=1)

# ---------- SUBCONJUNTOS ÚTILES ----------
df_tracks = df[df["match_type"].astype(str).str.lower() == "track"].copy()
df_winners = df[df["winner"] == True].copy()

# ---------- KPIs RÁPIDOS ----------
print("\n== KPIs ==")
print("Artistas únicos:", df["artist_main"].nunique())
if df_tracks["track_genre"].notna().any():
    print("Género top (tracks):", df_tracks["track_genre"].value_counts().idxmax())
if df_winners["artist_main"].notna().any():
    top_artist = df_winners["artist_main"].value_counts().idxmax()
    top_count  = df_winners["artist_main"].value_counts().max()
    print(f"Artista con más Grammys ganados: {top_artist} ({top_count})")
if df_tracks["popularity"].notna().any():
    print("Popularidad promedio (tracks):", round(df_tracks["popularity"].mean(), 2))

# %%
# 1) TOP 10 ARTISTAS CON MÁS GRAMMYS (BARRAS HORIZONTALES)
w = df_winners["artist_main"].dropna().value_counts().head(10)
if not w.empty:
    plt.figure(figsize=(8,5))
    sns.barplot(x=w.values, y=w.index, orient="h")
    plt.title("Top 10 artistas con más Grammys (ganados)")
    plt.xlabel("Cantidad de premios"); plt.ylabel("Artista")
    plt.tight_layout(); plt.show()

# %%
# 2) TOP 5 GÉNEROS PREMIADOS (TRACKS)
top_artists = df["artist"].value_counts().head(5)
plt.figure(figsize=(7, 4))
plt.bar(top_artists.index, top_artists.values, color="orange")
plt.title("Top 5 Artistas con Más Canciones")
plt.xlabel("Artista")
plt.ylabel("Cantidad de Canciones")
plt.xticks(rotation=30, ha="right")
plt.tight_layout()
plt.show()

# %%
sns.set_theme(style="whitegrid")

# --- Verificar columnas ---
for col in ["year", "popularity"]:
    if col not in df.columns:
        print(f"⚠ Columna faltante: '{col}', se creará vacía.")
        df[col] = np.nan

# --- Conversión segura ---
df["year"] = pd.to_numeric(df["year"], errors="coerce")
df["popularity"] = pd.to_numeric(df["popularity"], errors="coerce")

# --- Filtrado de años válidos ---
df_trends = df[(df["year"].notna()) & (df["year"] >= 1950)].copy()

if df_trends.empty:
    print("⚠ No hay datos válidos de año (>=1950). Mostrando primeros años detectados:")
    print(df["year"].dropna().sort_values().unique()[:10])

# --- Calcular tendencia ---
if "popularity" in df_trends.columns and df_trends["popularity"].notna().any():
    pop_trend = (
        df_trends.groupby("year", as_index=True)["popularity"]
        .mean()
        .sort_index()
        .dropna()
    )

    if not pop_trend.empty:
        plt.figure(figsize=(9, 4))
        sns.lineplot(
            x=pop_trend.index, y=pop_trend.values,
            marker="o", color="teal", linewidth=2
        )
        plt.title("Tendencia de Popularidad Promedio por Año", fontsize=13, weight="bold")
        plt.xlabel("Año", fontsize=11)
        plt.ylabel("Popularidad promedio", fontsize=11)
        plt.grid(True, linestyle="--", alpha=0.6)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
        print(f"✔ Gráfico generado con {len(pop_trend)} años válidos.")
    else:
        print("⚠ No se pudo generar el gráfico: todos los valores de popularidad son nulos.")
else:
    print("⚠ No hay columna 'popularity' con datos válidos para graficar.")

# %%

# %%


# %%
# 6) MATRIZ DE CORRELACIÓN (CLEAN)
num_cols = [c for c in ["popularity","danceability","energy","loudness","tempo"] if c in df.columns]
if num_cols:
    corr = df[num_cols].apply(pd.to_numeric, errors="coerce").corr()
    if corr.notna().any().any():
        plt.figure(figsize=(6,4.5))
        sns.heatmap(corr, vmin=-1, vmax=1, cmap="coolwarm", annot=True, fmt=".2f",
                    square=True, cbar_kws={"shrink": .8})
        plt.title("Matriz de correlación")
        plt.tight_layout(); plt.show()

print("\nListo: gráficas simples, legibles y enfocadas ✅")
