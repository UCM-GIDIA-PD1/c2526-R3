"""
analisis_fase4.py
-----------------
Análisis comparativo de distribuciones entre los datos de fase 3 (MINI.parquet)
y los nuevos datos de 2026 (final_2026.parquet).

Uso:
    uv run python src/analisis/analisis_fase4.py \
        --datos_fase3 <ruta_MINI.parquet> \
        --datos_nuevos <ruta_final_2026.parquet> \
        --output_dir <carpeta_salida>
"""

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np
import pandas as pd
from scipy import stats


# ──────────────────────────────────────────────
# Constantes
# ──────────────────────────────────────────────

VARIABLES_CLAVE = ["lon", "lat", "NDVI", "NDWI", "dist_civ"]
VARIABLE_OBJETIVO = "final"

COLORES = {
    "fase3": "#2563EB",   # azul
    "nuevos": "#DC2626",  # rojo
}

ETIQUETAS = {
    "fase3": "Fase 3 (2022-2025)",
    "nuevos": "Nuevos datos (2026)",
}


# ──────────────────────────────────────────────
# Carga de datos
# ──────────────────────────────────────────────

def cargar_datos(ruta_fase3: str, ruta_nuevos: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    print(f"Cargando datos fase 3 desde: {ruta_fase3}")
    df_fase3 = pd.read_parquet(ruta_fase3)
    print(f"  → {df_fase3.shape[0]:,} registros, {df_fase3.shape[1]} columnas")

    print(f"Cargando datos nuevos desde: {ruta_nuevos}")
    df_nuevos = pd.read_parquet(ruta_nuevos)
    print(f"  → {df_nuevos.shape[0]:,} registros, {df_nuevos.shape[1]} columnas")

    return df_fase3, df_nuevos


# ──────────────────────────────────────────────
# Análisis estadístico
# ──────────────────────────────────────────────

def resumen_estadistico(df_fase3: pd.DataFrame, df_nuevos: pd.DataFrame) -> pd.DataFrame:
    """Genera tabla comparativa de estadísticos básicos para las variables clave."""
    filas = []
    for var in VARIABLES_CLAVE:
        for nombre, df in [("Fase 3", df_fase3), ("2026", df_nuevos)]:
            filas.append({
                "Variable": var,
                "Dataset": nombre,
                "N": len(df[var]),
                "Media": df[var].mean(),
                "Std": df[var].std(),
                "Min": df[var].min(),
                "Max": df[var].max(),
            })
    return pd.DataFrame(filas)


def test_ks(df_fase3: pd.DataFrame, df_nuevos: pd.DataFrame) -> pd.DataFrame:
    """
    Test de Kolmogorov-Smirnov para detectar diferencias significativas
    en la distribución de cada variable clave.
    """
    filas = []
    for var in VARIABLES_CLAVE:
        stat, pval = stats.ks_2samp(df_fase3[var].dropna(), df_nuevos[var].dropna())
        filas.append({
            "Variable": var,
            "KS statistic": round(stat, 4),
            "p-valor": round(pval, 4),
            "¿Distribución diferente? (p<0.05)": "SÍ" if pval < 0.05 else "NO",
        })
    return pd.DataFrame(filas)


def resumen_objetivo(df_fase3: pd.DataFrame, df_nuevos: pd.DataFrame) -> None:
    """Imprime resumen de la variable objetivo (incendio/no incendio)."""
    print("\n" + "="*55)
    print("VARIABLE OBJETIVO: incendio (final = 1)")
    print("="*55)
    for nombre, df in [("Fase 3 (2022-2025)", df_fase3), ("Nuevos datos (2026)", df_nuevos)]:
        total = len(df)
        fuego = df[VARIABLE_OBJETIVO].sum()
        pct = fuego / total * 100
        print(f"\n  {nombre}")
        print(f"    Total registros : {total:,}")
        print(f"    Incendios (1)   : {fuego:,}  ({pct:.2f}%)")
        print(f"    No incendio (0) : {total - fuego:,}  ({100 - pct:.2f}%)")


# ──────────────────────────────────────────────
# Visualizaciones
# ──────────────────────────────────────────────

def grafica_objetivo(df_fase3: pd.DataFrame, df_nuevos: pd.DataFrame, output_dir: Path) -> None:
    """Gráfica comparativa de la variable objetivo."""
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    fig.suptitle("Variable objetivo: proporción de incendios", fontsize=13, fontweight="bold")

    for ax, (nombre, df, color) in zip(axes, [
        ("Fase 3 (2022-2025)", df_fase3, COLORES["fase3"]),
        ("Nuevos datos (2026)", df_nuevos, COLORES["nuevos"]),
    ]):
        counts = df[VARIABLE_OBJETIVO].value_counts().sort_index()
        labels = ["No incendio", "Incendio"]
        valores = [counts.get(0, 0), counts.get(1, 0)]
        bars = ax.bar(labels, valores, color=[color, color], edgecolor="white")
        bars[0].set_alpha(0.4)
        bars[1].set_alpha(1.0)
        ax.set_title(nombre, fontsize=11)
        ax.set_ylabel("Número de registros")
        for bar, val in zip(bars, valores):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 5,
                    f"{val:,}\n({val/sum(valores)*100:.1f}%)",
                    ha="center", va="bottom", fontsize=9)
        ax.set_ylim(0, max(valores) * 1.2)

    plt.tight_layout()
    ruta = output_dir / "objetivo_comparacion.png"
    plt.savefig(ruta, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Guardada: {ruta}")


def graficas_variables(df_fase3: pd.DataFrame, df_nuevos: pd.DataFrame, output_dir: Path) -> None:
    """Histogramas comparativos para cada variable clave."""
    fig, axes = plt.subplots(2, 3, figsize=(15, 8))
    fig.suptitle("Distribución de variables clave: Fase 3 vs. Datos 2026",
                 fontsize=13, fontweight="bold")
    axes_flat = axes.flatten()

    for i, var in enumerate(VARIABLES_CLAVE):
        ax = axes_flat[i]
        v3 = df_fase3[var].dropna()
        vn = df_nuevos[var].dropna()

        # Rango común
        xmin = min(v3.min(), vn.min())
        xmax = max(v3.max(), vn.max())
        bins = np.linspace(xmin, xmax, 40)

        ax.hist(v3, bins=bins, color=COLORES["fase3"], alpha=0.6,
                label=ETIQUETAS["fase3"], density=True)
        ax.hist(vn, bins=bins, color=COLORES["nuevos"], alpha=0.6,
                label=ETIQUETAS["nuevos"], density=True)

        # Líneas de media
        ax.axvline(v3.mean(), color=COLORES["fase3"], linestyle="--", linewidth=1.5,
                   label=f"Media F3: {v3.mean():.2f}")
        ax.axvline(vn.mean(), color=COLORES["nuevos"], linestyle="--", linewidth=1.5,
                   label=f"Media 2026: {vn.mean():.2f}")

        ax.set_title(var, fontsize=11, fontweight="bold")
        ax.set_ylabel("Densidad")
        ax.legend(fontsize=7)

    # Ocultar eje sobrante
    axes_flat[-1].set_visible(False)

    plt.tight_layout()
    ruta = output_dir / "variables_comparacion.png"
    plt.savefig(ruta, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Guardada: {ruta}")


def grafica_boxplots(df_fase3: pd.DataFrame, df_nuevos: pd.DataFrame, output_dir: Path) -> None:
    """Boxplots comparativos normalizados para ver diferencias de escala."""
    fig, axes = plt.subplots(1, len(VARIABLES_CLAVE), figsize=(16, 5))
    fig.suptitle("Boxplots comparativos por variable", fontsize=13, fontweight="bold")

    for ax, var in zip(axes, VARIABLES_CLAVE):
        datos = [df_fase3[var].dropna(), df_nuevos[var].dropna()]
        bp = ax.boxplot(datos, patch_artist=True, widths=0.5,
                        medianprops=dict(color="black", linewidth=2))
        bp["boxes"][0].set_facecolor(COLORES["fase3"])
        bp["boxes"][0].set_alpha(0.7)
        bp["boxes"][1].set_facecolor(COLORES["nuevos"])
        bp["boxes"][1].set_alpha(0.7)
        ax.set_title(var, fontsize=10, fontweight="bold")
        ax.set_xticks([1, 2])
        ax.set_xticklabels(["F3", "2026"], fontsize=8)

    plt.tight_layout()
    ruta = output_dir / "boxplots_comparacion.png"
    plt.savefig(ruta, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Guardada: {ruta}")


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Análisis comparativo Fase 3 vs. datos 2026")
    parser.add_argument(
        "--datos_fase3",
        type=str,
        default="data/MINI.parquet",
        help="Ruta al parquet de fase 3 (MINI.parquet)",
    )
    parser.add_argument(
        "--datos_nuevos",
        type=str,
        default="data/final_2026.parquet",
        help="Ruta al parquet de datos nuevos (final_2026.parquet)",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default="resultados/analisis_fase4",
        help="Carpeta donde guardar las gráficas y tablas",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Carga
    df_fase3, df_nuevos = cargar_datos(args.datos_fase3, args.datos_nuevos)

    # Análisis variable objetivo
    resumen_objetivo(df_fase3, df_nuevos)

    # Estadísticos
    print("\n" + "="*55)
    print("ESTADÍSTICOS COMPARATIVOS")
    print("="*55)
    tabla_stats = resumen_estadistico(df_fase3, df_nuevos)
    print(tabla_stats.to_string(index=False))
    tabla_stats.to_csv(output_dir / "estadisticos.csv", index=False)
    print(f"\n  Tabla guardada: {output_dir / 'estadisticos.csv'}")

    # Test KS
    print("\n" + "="*55)
    print("TEST KOLMOGOROV-SMIRNOV (diferencias de distribución)")
    print("="*55)
    tabla_ks = test_ks(df_fase3, df_nuevos)
    print(tabla_ks.to_string(index=False))
    tabla_ks.to_csv(output_dir / "test_ks.csv", index=False)
    print(f"\n  Tabla guardada: {output_dir / 'test_ks.csv'}")

    # Gráficas
    print("\n" + "="*55)
    print("GENERANDO GRÁFICAS")
    print("="*55)
    grafica_objetivo(df_fase3, df_nuevos, output_dir)
    graficas_variables(df_fase3, df_nuevos, output_dir)
    grafica_boxplots(df_fase3, df_nuevos, output_dir)

    print("\n Análisis completado. Archivos en:", output_dir)


if __name__ == "__main__":
    main()
