"""
analisis_fase4.py
-----------------
Análisis exploratorio del dataset combinado (MINI.parquet + final_2026.parquet):
- Nulos y calidad de datos
- Distribución geográfica de puntos e incendios
- Distribución temporal
- Distribución de variables clave
- Balance de clases

Uso:
    uv run python src/analisis/analisis_fase4.py \
        --datos_fase3 <ruta_MINI.parquet> \
        --datos_nuevos <ruta_final_2026.parquet> \
        --output_dir <carpeta_salida>
"""

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from extraccion import minioFunctions as mf

VARIABLE_OBJETIVO = "final"
VARIABLES_CLAVE = ["temp_mean", "humidity_mean", "NDVI", "NDWI", "dist_civ", "wind_speed_max"]

COLOR_NO_FUEGO = "#2563EB"
COLOR_FUEGO    = "#DC2626"


# ──────────────────────────────────────────────
# Carga
# ──────────────────────────────────────────────

def cargar_y_combinar(ruta_fase3: str, ruta_nuevos: str) -> pd.DataFrame:
    cliente = mf.crear_cliente()
    df_fase3 = mf.bajar_fichero(cliente, ruta_fase3)
    df_nuevos = mf.bajar_fichero(cliente, ruta_nuevos)

    df_fase3["origen"] = "Fase 3 (2022-2025)"
    df_nuevos["origen"] = "Nuevos (2026)"

    df = pd.concat([df_fase3, df_nuevos], ignore_index=True)
    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month

    print(f"Dataset combinado: {df.shape[0]:,} registros, {df.shape[1]} columnas")
    print(f"  Fase 3  : {len(df_fase3):,} registros")
    print(f"  2026    : {len(df_nuevos):,} registros")
    return df


# ──────────────────────────────────────────────
# 1. Análisis de nulos
# ──────────────────────────────────────────────

def analisis_nulos(df: pd.DataFrame, output_dir: Path) -> None:
    print("\n" + "="*50)
    print("1. ANÁLISIS DE NULOS")
    print("="*50)

    nulos = df.isnull().sum()
    pct = (nulos / len(df) * 100).round(2)
    tabla = pd.DataFrame({"Nulos": nulos, "% Nulos": pct})

    if tabla["Nulos"].sum() == 0:
        print("Sin valores nulos en ninguna columna.")
    else:
        print(tabla[tabla["Nulos"] > 0])

    cols = [c for c in df.columns if c not in ["date", "origen", "year", "month"]]
    valores = [df[c].isnull().sum() for c in cols]
    colores = [COLOR_FUEGO if v > 0 else COLOR_NO_FUEGO for v in valores]

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.bar(cols, valores, color=colores)
    ax.set_title("Valores nulos por columna", fontsize=13, fontweight="bold")
    ax.set_ylabel("Número de nulos")
    ax.set_xticklabels(cols, rotation=45, ha="right", fontsize=8)
    ax.axhline(0, color="black", linewidth=0.5)
    plt.tight_layout()
    ruta = output_dir / "1_nulos.png"
    plt.savefig(ruta, dpi=150, bbox_inches="tight")
    print(f"  Guardada: {ruta}")
    tabla.to_csv(output_dir / "1_nulos.csv")


# ──────────────────────────────────────────────
# 2. Distribución geográfica
# ──────────────────────────────────────────────

def analisis_geografico(df: pd.DataFrame, output_dir: Path) -> None:
    print("\n" + "="*50)
    print("2. DISTRIBUCIÓN GEOGRÁFICA")
    print("="*50)
    print(f"  Latitud  : {df['lat'].min():.2f} — {df['lat'].max():.2f}")
    print(f"  Longitud : {df['lon'].min():.2f} — {df['lon'].max():.2f}")
    print(f"  Registros con incendio: {df[VARIABLE_OBJETIVO].sum():,}")

    no_fuego = df[df[VARIABLE_OBJETIVO] == 0]
    fuego    = df[df[VARIABLE_OBJETIVO] == 1]

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle("Distribución geográfica de registros e incendios", fontsize=13, fontweight="bold")

    ax = axes[0]
    ax.scatter(no_fuego["lon"], no_fuego["lat"], s=1, alpha=0.1,
               color=COLOR_NO_FUEGO, label=f"No incendio ({len(no_fuego):,})")
    ax.scatter(fuego["lon"], fuego["lat"], s=8, alpha=0.7,
               color=COLOR_FUEGO, label=f"Incendio ({len(fuego):,})")
    ax.set_title("Todos los registros")
    ax.set_xlabel("Longitud")
    ax.set_ylabel("Latitud")
    ax.legend(fontsize=8, markerscale=4)

    ax = axes[1]
    for origen, color in [("Fase 3 (2022-2025)", COLOR_NO_FUEGO), ("Nuevos (2026)", COLOR_FUEGO)]:
        sub = fuego[fuego["origen"] == origen]
        ax.scatter(sub["lon"], sub["lat"], s=15, alpha=0.8,
                   color=color, label=f"{origen} ({len(sub)})")
    ax.set_title("Incendios por origen del dato")
    ax.set_xlabel("Longitud")
    ax.set_ylabel("Latitud")
    ax.legend(fontsize=8, markerscale=2)

    plt.tight_layout()
    ruta = output_dir / "2_distribucion_geografica.png"
    plt.savefig(ruta, dpi=150, bbox_inches="tight")
    print(f"  Guardada: {ruta}")


# ──────────────────────────────────────────────
# 3. Distribución temporal
# ──────────────────────────────────────────────

def analisis_temporal(df: pd.DataFrame, output_dir: Path) -> None:
    print("\n" + "="*50)
    print("3. DISTRIBUCIÓN TEMPORAL")
    print("="*50)

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle("Distribución temporal de registros e incendios", fontsize=13, fontweight="bold")

    ax = axes[0]
    por_año = df.groupby("year")[VARIABLE_OBJETIVO].agg(["count", "sum"]).reset_index()
    por_año.columns = ["year", "total", "incendios"]
    x = np.arange(len(por_año))
    w = 0.4
    ax.bar(x - w/2, por_año["total"], width=w, label="Total registros",
           color=COLOR_NO_FUEGO, alpha=0.7)
    ax.bar(x + w/2, por_año["incendios"], width=w, label="Incendios",
           color=COLOR_FUEGO, alpha=0.9)
    ax.set_xticks(x)
    ax.set_xticklabels(por_año["year"])
    ax.set_title("Registros e incendios por año")
    ax.set_ylabel("Número de registros")
    ax.legend()

    ax = axes[1]
    por_mes = df[df[VARIABLE_OBJETIVO] == 1].groupby("month").size().reindex(range(1, 13), fill_value=0)
    meses = ["Ene", "Feb", "Mar", "Abr", "May", "Jun",
              "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
    ax.bar(meses, por_mes.values, color=COLOR_FUEGO, alpha=0.85)
    ax.set_title("Estacionalidad de incendios (todos los años)")
    ax.set_ylabel("Número de incendios")
    ax.set_xlabel("Mes")

    plt.tight_layout()
    ruta = output_dir / "3_distribucion_temporal.png"
    plt.savefig(ruta, dpi=150, bbox_inches="tight")
    print(f"  Guardada: {ruta}")


# ──────────────────────────────────────────────
# 4. Distribución de variables clave
# ──────────────────────────────────────────────

def analisis_variables(df: pd.DataFrame, output_dir: Path) -> None:
    print("\n" + "="*50)
    print("4. DISTRIBUCIÓN DE VARIABLES CLAVE")
    print("="*50)

    no_fuego = df[df[VARIABLE_OBJETIVO] == 0]
    fuego    = df[df[VARIABLE_OBJETIVO] == 1]

    ncols = 3
    nrows = (len(VARIABLES_CLAVE) + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(15, 5 * nrows))
    fig.suptitle("Distribución de variables clave: incendio vs. no incendio",
                 fontsize=13, fontweight="bold")
    axes_flat = axes.flatten()

    for i, var in enumerate(VARIABLES_CLAVE):
        ax = axes_flat[i]
        v_no = no_fuego[var].dropna()
        v_si = fuego[var].dropna()
        xmin = min(v_no.quantile(0.01), v_si.quantile(0.01))
        xmax = max(v_no.quantile(0.99), v_si.quantile(0.99))
        bins = np.linspace(xmin, xmax, 40)
        ax.hist(v_no, bins=bins, color=COLOR_NO_FUEGO, alpha=0.6, density=True,
                label=f"No incendio (n={len(v_no):,})")
        ax.hist(v_si, bins=bins, color=COLOR_FUEGO, alpha=0.7, density=True,
                label=f"Incendio (n={len(v_si):,})")
        ax.axvline(v_no.mean(), color=COLOR_NO_FUEGO, linestyle="--", linewidth=1.5)
        ax.axvline(v_si.mean(), color=COLOR_FUEGO, linestyle="--", linewidth=1.5)
        ax.set_title(var, fontsize=11, fontweight="bold")
        ax.set_ylabel("Densidad")
        ax.legend(fontsize=7)

    for j in range(i + 1, len(axes_flat)):
        axes_flat[j].set_visible(False)

    plt.tight_layout()
    ruta = output_dir / "4_variables_clave.png"
    plt.savefig(ruta, dpi=150, bbox_inches="tight")
    print(f"  Guardada: {ruta}")


# ──────────────────────────────────────────────
# 5. Balance de clases
# ──────────────────────────────────────────────

def analisis_balance(df: pd.DataFrame, output_dir: Path) -> None:
    print("\n" + "="*50)
    print("5. BALANCE DE CLASES")
    print("="*50)
    total = len(df)
    fuego = int(df[VARIABLE_OBJETIVO].sum())
    print(f"  Total    : {total:,}")
    print(f"  Incendio : {fuego:,}  ({fuego/total*100:.2f}%)")
    print(f"  No incen.: {total-fuego:,}  ({(total-fuego)/total*100:.2f}%)")
    print(f"  Ratio    : 1 incendio por cada {(total-fuego)//fuego} no-incendios")

    fig, ax = plt.subplots(figsize=(6, 5))
    labels = ["No incendio", "Incendio"]
    valores = [total - fuego, fuego]
    colores = [COLOR_NO_FUEGO, COLOR_FUEGO]
    bars = ax.bar(labels, valores, color=colores, alpha=0.85, edgecolor="white")
    for bar, val in zip(bars, valores):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 200,
                f"{val:,}\n({val/total*100:.1f}%)",
                ha="center", va="bottom", fontsize=10)
    ax.set_title("Balance de clases — dataset combinado", fontsize=13, fontweight="bold")
    ax.set_ylabel("Número de registros")
    ax.set_ylim(0, max(valores) * 1.15)
    plt.tight_layout()
    ruta = output_dir / "5_balance_clases.png"
    plt.savefig(ruta, dpi=150, bbox_inches="tight")
    print(f"  Guardada: {ruta}")


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Análisis exploratorio dataset combinado — Fase 4")
    parser.add_argument("--output_dir", type=str, default="resultados/analisis_fase4",
                        help="Carpeta donde guardar las gráficas y tablas")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    df = cargar_y_combinar("grupo3/cleaned/MINI.parquet", "grupo3/cleaned/final_cleaned_2026.parquet")

    analisis_nulos(df, output_dir)
    analisis_geografico(df, output_dir)
    analisis_temporal(df, output_dir)
    analisis_variables(df, output_dir)
    analisis_balance(df, output_dir)

    plt.show()
    plt.close()
    
    print(f"\n Análisis completado. Archivos en: {output_dir}")


if __name__ == "__main__":
    main()
    
