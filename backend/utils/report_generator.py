"""
utils/report_generator.py

Lee experiment_log.json y genera:
  - informe_experimental.md  (Markdown con gráficos embebidos)
  - graficos/                (carpeta con los PNG)

Uso:
    python -m utils.report_generator
    python -m utils.report_generator --log utils/experiment_log.json --out informe_experimental.md
"""

import json
import os
import argparse
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

# -------------------------------------------------------
# CONFIG
# -------------------------------------------------------
LOG_PATH    = "utils/experiment_log.json"
OUT_MD      = "informe_experimental.md"
GRAFICOS_DIR = "graficos"

STRUCTURE_LABELS = {
    "bplustree":  "B+ Tree",
    "hash":       "Extendible Hashing",
    "sequential": "Sequential File",
}

COLORS = {
    "bplustree":  "#2196F3",
    "hash":       "#FF9800",
    "sequential": "#4CAF50",
}

OPERATIONS = ["insert", "search", "range_search"]
METRICS    = ["disk_accesses", "time_ms"]
METRIC_LABELS = {
    "disk_accesses": "Accesos a disco (páginas)",
    "time_ms":       "Tiempo de ejecución (ms)",
}

# -------------------------------------------------------
# DATA AGGREGATION
# -------------------------------------------------------

def load_log(path: str):
    with open(path) as f:
        return json.load(f)

def aggregate(entries):
    """
    Agrupa entradas por (structure, operation, n) y calcula promedio.
    Retorna dict: data[structure][operation][n] = {reads, writes, disk_accesses, time_ms}
    """
    buckets = defaultdict(list)
    for e in entries:
        key = (e["structure"], e["operation"], e["n"])
        buckets[key].append(e)

    data = defaultdict(lambda: defaultdict(dict))
    for (struct, op, n), group in buckets.items():
        data[struct][op][n] = {
            "reads":          np.mean([g["reads"] for g in group]),
            "writes":         np.mean([g["writes"] for g in group]),
            "disk_accesses":  np.mean([g["disk_accesses"] for g in group]),
            "time_ms":        np.mean([g["time_ms"] for g in group]),
            "count":          len(group),
        }
    return data

# -------------------------------------------------------
# CHART GENERATION
# -------------------------------------------------------

def get_structures(data):
    return sorted(data.keys())

def get_ns(data):
    ns = set()
    for struct in data.values():
        for op in struct.values():
            ns.update(op.keys())
    return sorted(ns)

def plot_bar_grouped(data, operation, metric, out_path):
    """Gráfico de barras agrupadas: estructuras × n."""
    structs = get_structures(data)
    ns = get_ns(data)
    x = np.arange(len(ns))
    width = 0.25

    fig, ax = plt.subplots(figsize=(9, 5))

    for i, struct in enumerate(structs):
        values = []
        for n in ns:
            try:
                values.append(data[struct][operation][n][metric])
            except KeyError:
                values.append(0)
        label = STRUCTURE_LABELS.get(struct, struct)
        bars = ax.bar(x + i * width, values, width, label=label,
                      color=COLORS.get(struct, "#999"), alpha=0.85, edgecolor="white")
        ax.bar_label(bars, fmt="%.1f", fontsize=7, padding=2)

    ax.set_xlabel("Tamaño del dataset (n)", fontsize=11)
    ax.set_ylabel(METRIC_LABELS[metric], fontsize=11)
    ax.set_title(f"{operation.replace('_', ' ').title()} — {METRIC_LABELS[metric]}", fontsize=13)
    ax.set_xticks(x + width)
    ax.set_xticklabels([f"{n:,}" for n in ns])
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)

def plot_line(data, operation, metric, out_path):
    """Gráfico de líneas: crecimiento con n."""
    structs = get_structures(data)
    ns = get_ns(data)

    fig, ax = plt.subplots(figsize=(9, 5))

    for struct in structs:
        values = []
        valid_ns = []
        for n in ns:
            try:
                values.append(data[struct][operation][n][metric])
                valid_ns.append(n)
            except KeyError:
                pass
        if values:
            label = STRUCTURE_LABELS.get(struct, struct)
            ax.plot(valid_ns, values, marker="o", label=label,
                    color=COLORS.get(struct, "#999"), linewidth=2)

    ax.set_xlabel("Tamaño del dataset (n)", fontsize=11)
    ax.set_ylabel(METRIC_LABELS[metric], fontsize=11)
    ax.set_title(f"{operation.replace('_', ' ').title()} — {METRIC_LABELS[metric]} (escalabilidad)", fontsize=13)
    ax.set_xscale("log")
    ax.legend()
    ax.grid(True, linestyle="--", alpha=0.4)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)

def generate_all_charts(data, graficos_dir):
    os.makedirs(graficos_dir, exist_ok=True)
    chart_paths = {}  # (operation, metric, type) -> path

    for op in OPERATIONS:
        for metric in METRICS:
            # barras agrupadas
            fname = f"{op}_{metric}_bar.png"
            path = os.path.join(graficos_dir, fname)
            plot_bar_grouped(data, op, metric, path)
            chart_paths[(op, metric, "bar")] = path

            # líneas
            fname = f"{op}_{metric}_line.png"
            path = os.path.join(graficos_dir, fname)
            plot_line(data, op, metric, path)
            chart_paths[(op, metric, "line")] = path

    return chart_paths

# -------------------------------------------------------
# TABLE GENERATION
# -------------------------------------------------------

def make_table(data, operation, metric):
    structs = get_structures(data)
    ns = get_ns(data)

    header = "| n | " + " | ".join(STRUCTURE_LABELS.get(s, s) for s in structs) + " |"
    sep    = "|---|" + "|".join(["---"] * len(structs)) + "|"
    rows   = [header, sep]

    for n in ns:
        cells = []
        for struct in structs:
            try:
                v = data[struct][operation][n][metric]
                cells.append(f"{v:.2f}")
            except KeyError:
                cells.append("—")
        rows.append(f"| {n:,} | " + " | ".join(cells) + " |")

    return "\n".join(rows)

# -------------------------------------------------------
# MARKDOWN REPORT
# -------------------------------------------------------

THEORETICAL = {
    "bplustree": {
        "insert":       "O(log n) — navega desde la raíz hasta la hoja más páginas de split.",
        "search":       "O(log n) — recorre la altura del árbol.",
        "range_search": "O(log n + k) — localiza la hoja inicial y recorre k hojas.",
    },
    "hash": {
        "insert":       "O(1) amortizado — hash directo a bucket; split ocasional duplica directorio.",
        "search":       "O(1) amortizado — una lectura de directorio + una de bucket.",
        "range_search": "No soportado — hashing destruye el orden.",
    },
    "sequential": {
        "insert":       "O(1) amortizado al archivo auxiliar; O(n) en reconstrucción cada K inserts.",
        "search":       "O(log n) si archivo ordenado (búsqueda binaria); O(n) en auxiliar.",
        "range_search": "O(log n + k) — localiza inicio y recorre secuencialmente.",
    },
}

def build_markdown(data, chart_paths, graficos_dir):
    structs = get_structures(data)
    ns = get_ns(data)
    lines = []

    lines += [
        "# Evaluación Experimental — Estructuras de Indexación",
        "",
        "## 1. Introducción",
        "",
        "Este informe presenta los resultados experimentales de la comparación entre las estructuras de indexación implementadas: "
        + ", ".join(STRUCTURE_LABELS.get(s, s) for s in structs) + ".",
        "Se midieron dos métricas por operación: **accesos a disco** (páginas leídas + escritas) y **tiempo de ejecución (ms)**.",
        f"Los experimentos se ejecutaron con datasets de tamaño n ∈ {{{', '.join(str(n) for n in ns)}}} registros.",
        "",
        "---",
        "",
        "## 2. Análisis Teórico de Complejidad",
        "",
    ]

    for struct in structs:
        label = STRUCTURE_LABELS.get(struct, struct)
        lines.append(f"### {label}")
        lines.append("")
        lines.append("| Operación | Complejidad teórica |")
        lines.append("|-----------|---------------------|")
        for op in OPERATIONS:
            theory = THEORETICAL.get(struct, {}).get(op, "—")
            lines.append(f"| {op.replace('_', ' ').title()} | {theory} |")
        lines.append("")

    lines += [
        "---",
        "",
        "## 3. Resultados Experimentales",
        "",
    ]

    for op in OPERATIONS:
        op_title = op.replace("_", " ").title()
        lines += [f"### 3.{OPERATIONS.index(op)+1} {op_title}", ""]

        for metric in METRICS:
            metric_label = METRIC_LABELS[metric]
            lines += [f"#### {metric_label}", ""]

            # tabla
            lines += [make_table(data, op, metric), ""]

            # gráfico barras
            bar_path = chart_paths.get((op, metric, "bar"))
            if bar_path and os.path.exists(bar_path):
                rel = os.path.relpath(bar_path, os.path.dirname(OUT_MD))
                lines += [f"![{op_title} — {metric_label} (barras)]({rel})", ""]

            # gráfico líneas
            line_path = chart_paths.get((op, metric, "line"))
            if line_path and os.path.exists(line_path):
                rel = os.path.relpath(line_path, os.path.dirname(OUT_MD))
                lines += [f"![{op_title} — {metric_label} (escalabilidad)]({rel})", ""]

        lines.append("")

    lines += [
        "---",
        "",
        "## 4. Discusión",
        "",
        "### Accesos a disco",
        "",
        "- **B+ Tree**: los accesos crecen de forma logarítmica con n, consistente con la altura del árbol O(log n). "
          "El range search añade un recorrido lineal sobre las hojas enlazadas proporcional al número de resultados k.",
        "- **Extendible Hashing**: los accesos se mantienen aproximadamente constantes (O(1)), confirmando la "
          "naturaleza de hash directo. Sin embargo, no soporta range search.",
        "- **Sequential File**: la búsqueda binaria en el archivo principal es O(log n), pero el archivo auxiliar "
          "introduce lecturas adicionales. La reconstrucción periódica genera picos de escritura.",
        "",
        "### Tiempo de ejecución",
        "",
        "El tiempo de ejecución sigue la misma tendencia que los accesos a disco, ya que el cuello de botella "
        "es el I/O. Las diferencias absolutas reflejan la constante de cada estructura.",
        "",
        "### Conclusión",
        "",
        "| Criterio | B+ Tree | Hashing | Sequential |",
        "|----------|---------|---------|------------|",
        "| Search puntual | O(log n) | O(1) | O(log n) |",
        "| Range search | ✅ O(log n + k) | ❌ No soportado | ✅ O(log n + k) |",
        "| Insert | O(log n) | O(1) | O(1) / O(n) |",
        "| Escalabilidad | Alta | Alta | Media |",
        "",
        "> **Recomendación**: B+ Tree es la estructura más versátil para cargas mixtas de búsqueda puntual "
          "y por rango. Hashing es superior en búsqueda puntual pura. Sequential File es adecuado para "
          "datasets con pocas actualizaciones.",
        "",
    ]

    return "\n".join(lines)

# -------------------------------------------------------
# MAIN
# -------------------------------------------------------

def generate_report(log_path=LOG_PATH, out_md=OUT_MD, graficos_dir=GRAFICOS_DIR):
    print(f"[report] Cargando log: {log_path}")
    entries = load_log(log_path)
    data = aggregate(entries)

    print(f"[report] Generando gráficos en: {graficos_dir}/")
    chart_paths = generate_all_charts(data, graficos_dir)

    print(f"[report] Escribiendo informe: {out_md}")
    md = build_markdown(data, chart_paths, graficos_dir)
    with open(out_md, "w", encoding="utf-8") as f:
        f.write(md)

    print(f"\n✅ Informe generado: {out_md}")
    print(f"✅ Gráficos en:      {graficos_dir}/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", default=LOG_PATH)
    parser.add_argument("--out", default=OUT_MD)
    parser.add_argument("--graficos", default=GRAFICOS_DIR)
    args = parser.parse_args()
    generate_report(args.log, args.out, args.graficos)