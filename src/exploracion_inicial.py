"""
exploracion_inicial.py
----------------------
Script de exploración rápida del dataset limpio de IgnisAI.
Carga los parquets de MinIO y hace comprobaciones básicas antes de modelar.

Uso:
    uv run python src/exploracion_inicial.py

Requiere variables de entorno en .env:
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
"""

import os
import sys
import io
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from minio import Minio
from dotenv import load_dotenv

# Añadir src al path para poder importar minioFunctions
sys.path.append(os.path.join(os.path.dirname(__file__), "extraccion"))
from minioFunctions import crear_cliente, bajar_fichero

# ── Configuración ──────────────────────────────────────────────────────────────

PREFIX_GENERAL   = "grupo3/cleaned/modelo_General"
PREFIX_INCENDIOS = "grupo3/cleaned/Modelo_Incendios"
YEARS            = [2022, 2023, 2024, 2025]
SEED             = 42

load_dotenv()


# ── Carga de datos ─────────────────────────────────────────────────────────────

def cargar_parquets(prefix: str, years: list[int]) -> pd.DataFrame:
    """Descarga y concatena los parquets de varios años desde MinIO."""
    cliente = crear_cliente()
    dfs = []
    for year in years:
        key = f"{prefix}_{year}.parquet"
        print(f"  Descargando {key} ...")
        df = bajar_fichero(cliente, key)
        if df is not None:
            df["_year"] = year
            dfs.append(df)
        else:
            print(f"  ⚠️  No se pudo descargar {key}")
    if not dfs:
        raise RuntimeError("No se cargó ningún archivo. ¿Tienes la VPN de la UCM activa?")
    return pd.concat(dfs, ignore_index=True)


# ── Análisis ───────────────────────────────────────────────────────────────────

def analizar_general(df: pd.DataFrame):
    """Comprobaciones sobre el dataset de clasificación (incendios + no incendios)."""

    print("\n" + "="*60)
    print("DATASET GENERAL (clasificación)")
    print("="*60)

    # Tamaño
    print(f"\nFilas totales : {len(df):,}")
    print(f"Columnas      : {df.shape[1]}")
    print(f"\nColumnas disponibles:\n  {list(df.columns)}")

    # Nulos
    nulos = df.isnull().sum()
    nulos_pct = (nulos / len(df) * 100).round(2)
    nulos_df = pd.DataFrame({"nulos": nulos, "porcentaje": nulos_pct})
    nulos_df = nulos_df[nulos_df["nulos"] > 0].sort_values("porcentaje", ascending=False)

    if nulos_df.empty:
        print("\n✅ Sin nulos residuales.")
    else:
        print(f"\n⚠️  Columnas con nulos:\n{nulos_df}")

    # Desbalanceo de clases
    if "final" not in df.columns:
        print("\n⚠️  Columna 'final' no encontrada. Revisa el nombre de la variable objetivo.")
        return

    conteo = df["final"].value_counts()
    pct    = df["final"].value_counts(normalize=True) * 100

    print("\n── Desbalanceo de clases ─────────────────────────")
    print(f"  No incendios (0): {conteo.get(0,0):>8,}  ({pct.get(0,0):.1f}%)")
    print(f"  Incendios    (1): {conteo.get(1,0):>8,}  ({pct.get(1,0):.1f}%)")
    ratio = conteo.get(0, 1) / max(conteo.get(1, 1), 1)
    print(f"  Ratio 0/1       : {ratio:.1f}x")

    if ratio > 5:
        print("  ⚠️  Clases muy desbalanceadas → usa estratificación y/o class_weight='balanced'")

    # Distribución por año
    if "_year" in df.columns:
        print("\n── Observaciones por año ─────────────────────────")
        print(df.groupby(["_year", "final"]).size().unstack(fill_value=0).to_string())

    # Estadísticas básicas de variables numéricas
    print("\n── Estadísticas descriptivas ─────────────────────")
    print(df.describe().round(3).to_string())

    # Correlaciones altas (posible multicolinealidad)
    num_cols = df.select_dtypes(include=np.number).columns.tolist()
    corr = df[num_cols].corr().abs()
    upper = corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))
    pares_altos = [(c, r, upper.at[r, c]) 
                   for c in upper.columns for r in upper.index 
                   if pd.notna(upper.at[r, c]) and upper.at[r, c] > 0.85]
    pares_altos.sort(key=lambda x: -x[2])

    print("\n── Pares con correlación > 0.85 (posible multicolinealidad) ──")
    if pares_altos:
        for c1, c2, val in pares_altos[:10]:
            print(f"  {c1:35s} ↔ {c2:35s}  r={val:.3f}")
    else:
        print("  Ninguno encontrado.")

    return df


def analizar_incendios(df: pd.DataFrame):
    """Comprobaciones sobre el dataset de regresión (solo incendios, variable FRP)."""

    print("\n" + "="*60)
    print("DATASET INCENDIOS (regresión FRP)")
    print("="*60)

    print(f"\nFilas totales : {len(df):,}")
    print(f"Columnas      : {df.shape[1]}")

    # Nulos
    nulos = df.isnull().sum()
    nulos_df = pd.DataFrame({"nulos": nulos, "pct": nulos/len(df)*100}).query("nulos > 0")
    if nulos_df.empty:
        print("\n✅ Sin nulos residuales.")
    else:
        print(f"\n⚠️  Columnas con nulos:\n{nulos_df}")

    # Distribución del FRP
    frp_col = None
    for c in ["frp_mean", "frp_sum", "frp", "FRP"]:
        if c in df.columns:
            frp_col = c
            break

    if frp_col is None:
        print("\n⚠️  No se encontró columna FRP. Columnas disponibles:", list(df.columns))
        return

    frp = df[frp_col].dropna()
    print(f"\n── Distribución de {frp_col} ──────────────────────")
    print(f"  Media   : {frp.mean():.2f} MW")
    print(f"  Mediana : {frp.median():.2f} MW")
    print(f"  Std     : {frp.std():.2f} MW")
    print(f"  Min     : {frp.min():.2f} MW")
    print(f"  Max     : {frp.max():.2f} MW")
    print(f"  Skewness: {frp.skew():.2f}  (>1 = muy sesgado a la derecha)")
    print(f"  Percentil 90: {frp.quantile(0.90):.2f} MW")
    print(f"  Percentil 99: {frp.quantile(0.99):.2f} MW")

    if frp.skew() > 1:
        print("\n  ⚠️  FRP muy sesgado → considera transformación log(FRP) para regresión lineal")

    return df, frp_col


# ── Visualizaciones ────────────────────────────────────────────────────────────

def generar_graficas(df_general, df_incendios, frp_col, output_dir="outputs"):
    os.makedirs(output_dir, exist_ok=True)

    fig = plt.figure(figsize=(16, 10))
    fig.suptitle("IgnisAI — Exploración inicial del dataset", fontsize=14, fontweight="bold")
    gs = gridspec.GridSpec(2, 3, figure=fig, hspace=0.4, wspace=0.35)

    # 1. Desbalanceo de clases
    ax1 = fig.add_subplot(gs[0, 0])
    conteo = df_general["final"].value_counts()
    ax1.bar(["No incendio (0)", "Incendio (1)"], 
            [conteo.get(0,0), conteo.get(1,0)],
            color=["steelblue", "tomato"])
    ax1.set_title("Distribución de clases")
    ax1.set_ylabel("Observaciones")
    for i, v in enumerate([conteo.get(0,0), conteo.get(1,0)]):
        ax1.text(i, v + 100, f"{v:,}", ha="center", fontsize=9)

    # 2. FRP distribución original
    ax2 = fig.add_subplot(gs[0, 1])
    frp = df_incendios[frp_col].dropna()
    ax2.hist(frp, bins=60, color="tomato", edgecolor="white", alpha=0.8)
    ax2.set_title(f"Distribución de {frp_col}")
    ax2.set_xlabel("MW")
    ax2.set_ylabel("Frecuencia")

    # 3. FRP transformado (log)
    ax3 = fig.add_subplot(gs[0, 2])
    log_frp = np.log1p(frp)
    ax3.hist(log_frp, bins=60, color="darkorange", edgecolor="white", alpha=0.8)
    ax3.set_title(f"log(1 + {frp_col})")
    ax3.set_xlabel("log(MW)")
    ax3.set_ylabel("Frecuencia")

    # 4. Observaciones por año
    if "_year" in df_general.columns:
        ax4 = fig.add_subplot(gs[1, 0])
        por_year = df_general.groupby(["_year", "final"]).size().unstack(fill_value=0)
        por_year.plot(kind="bar", ax=ax4, color=["steelblue", "tomato"], 
                      legend=True, rot=0)
        ax4.set_title("Observaciones por año")
        ax4.set_xlabel("Año")
        ax4.legend(["No incendio", "Incendio"])

    # 5. Matriz de correlación (top variables numéricas)
    ax5 = fig.add_subplot(gs[1, 1:])
    num_cols = df_general.select_dtypes(include=np.number).columns[:12]
    corr = df_general[num_cols].corr()
    im = ax5.imshow(corr, cmap="RdBu_r", vmin=-1, vmax=1)
    ax5.set_xticks(range(len(num_cols)))
    ax5.set_yticks(range(len(num_cols)))
    ax5.set_xticklabels(num_cols, rotation=45, ha="right", fontsize=7)
    ax5.set_yticklabels(num_cols, fontsize=7)
    ax5.set_title("Correlaciones (primeras 12 vars numéricas)")
    plt.colorbar(im, ax=ax5, fraction=0.03)

    ruta = os.path.join(output_dir, "exploracion_inicial.png")
    plt.savefig(ruta, dpi=150, bbox_inches="tight")
    print(f"\n✅ Gráficas guardadas en: {ruta}")
    plt.close()


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Exploración inicial del dataset IgnisAI")
    parser.add_argument("--years", nargs="+", type=int, default=YEARS,
                        help="Años a cargar (ej: --years 2022 2023)")
    parser.add_argument("--output-dir", default="outputs",
                        help="Carpeta donde guardar las gráficas")
    parser.add_argument("--no-graficas", action="store_true",
                        help="No generar gráficas")
    args = parser.parse_args()

    print("Conectando a MinIO... (asegúrate de tener la VPN de la UCM activa)")

    print("\nCargando dataset general (clasificación)...")
    df_general = cargar_parquets(PREFIX_GENERAL, args.years)

    print("\nCargando dataset incendios (regresión)...")
    df_incendios = cargar_parquets(PREFIX_INCENDIOS, args.years)

    analizar_general(df_general)
    resultado = analizar_incendios(df_incendios)

    if resultado and not args.no_graficas:
        _, frp_col = resultado
        generar_graficas(df_general, df_incendios, frp_col, args.output_dir)

    print("\n✅ Exploración completada.")


if __name__ == "__main__":
    main()