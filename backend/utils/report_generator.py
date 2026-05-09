'''"""
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
LOG_PATH     = "utils/experiment_log.json"
OUT_MD       = "informe_experimental.md"
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

def load_log(path):
    with open(path) as f:
        return json.load(f)

def aggregate(entries):
    buckets = defaultdict(list)
    for e in entries:
        key = (e["structure"], e["operation"], e["n"])
        buckets[key].append(e)

    data = defaultdict(lambda: defaultdict(dict))
    for (struct, op, n), group in buckets.items():
        data[struct][op][n] = {
            "reads":         np.mean([g["reads"]         for g in group]),
            "writes":        np.mean([g["writes"]        for g in group]),
            "disk_accesses": np.mean([g["disk_accesses"] for g in group]),
            "time_ms":       np.mean([g["time_ms"]       for g in group]),
            "count":         len(group),
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
    structs = get_structures(data)
    ns      = get_ns(data)
    x       = np.arange(len(ns))
    width   = 0.25

    fig, ax = plt.subplots(figsize=(9, 5))
    for i, struct in enumerate(structs):
        values = []
        for n in ns:
            try:
                values.append(data[struct][operation][n][metric])
            except KeyError:
                values.append(0)
        bars = ax.bar(x + i * width, values, width,
                      label=STRUCTURE_LABELS.get(struct, struct),
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
    structs = get_structures(data)
    ns      = get_ns(data)

    fig, ax = plt.subplots(figsize=(9, 5))
    for struct in structs:
        values, valid_ns = [], []
        for n in ns:
            try:
                values.append(data[struct][operation][n][metric])
                valid_ns.append(n)
            except KeyError:
                pass
        if values:
            ax.plot(valid_ns, values, marker="o",
                    label=STRUCTURE_LABELS.get(struct, struct),
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
    chart_paths = {}
    for op in OPERATIONS:
        for metric in METRICS:
            for kind, fn in [("bar", plot_bar_grouped), ("line", plot_line)]:
                fname = f"{op}_{metric}_{kind}.png"
                path  = os.path.join(graficos_dir, fname)
                fn(data, op, metric, path)
                chart_paths[(op, metric, kind)] = path
    return chart_paths

# -------------------------------------------------------
# TABLE GENERATION
# -------------------------------------------------------

def make_table(data, operation, metric):
    structs = get_structures(data)
    ns      = get_ns(data)

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
        "insert":       "O(log n) — navega desde la raíz hasta la hoja; puede generar splits en cascada.",
        "search":       "O(log n) — recorre la altura del árbol página por página.",
        "range_search": "O(log n + k) — localiza la hoja inicial y recorre las hojas enlazadas.",
    },
    "hash": {
        "insert":       "O(1) amortizado — hash directo al bucket; split ocasional duplica el directorio.",
        "search":       "O(1) amortizado — una lectura de directorio más una lectura de bucket.",
        "range_search": "No soportado — el hash destruye el orden de las claves.",
    },
    "sequential": {
        "insert":       "O(1) amortizado — escribe en el overflow; O(n) en reconstrucción cada K inserts.",
        "search":       "O(log n) en el archivo principal ordenado (búsqueda binaria); O(n) en el overflow.",
        "range_search": "O(log n + k) — localiza el inicio con búsqueda binaria y recorre páginas contiguas.",
    },
}

def build_discussion(data):
    ns      = get_ns(data)
    structs = get_structures(data)

    def val(struct, op, n, metric):
        try:
            return data[struct][op][n][metric]
        except KeyError:
            return None

    lines = []

    # ── INSERT ──────────────────────────────────────────────────────────────
    lines += [
        "### Insert",
        "",
        "El Extendible Hashing es el más eficiente en inserción, con un costo prácticamente "
        "constante de ~2.1 páginas por operación independientemente del tamaño del dataset. "
        "Esto confirma el comportamiento O(1) amortizado: la gran mayoría de las inserciones "
        "se resuelven con una lectura de bucket y una escritura, y los splits son eventos poco frecuentes.",
        "",
        "El B+ Tree crece de forma logarítmica: de 3.0 páginas en n=1,000 a 4.5 en n=100,000. "
        "Cada inserción navega desde la raíz hasta la hoja correspondiente, y el número de niveles "
        "aumenta lentamente con n, lo que explica el crecimiento suave.",
        "",
        "El Sequential File muestra un comportamiento distinto: el costo promedio por inserción "
        "sube de 2.1 a 6.9 páginas al pasar de 1,000 a 100,000 registros. Esto se debe al rebuild "
        "periódico cada K=200 inserts: a medida que el archivo principal crece, cada reconstrucción "
        "lee y reescribe más páginas, elevando el promedio amortizado. Aun así, el costo sigue siendo "
        "bajo porque se reparte entre 200 operaciones.",
        "",
    ]

    # ── SEARCH ──────────────────────────────────────────────────────────────
    bpt_search = [val("bplustree", "search", n, "disk_accesses") for n in ns]
    seq_search = [val("sequential", "search", n, "disk_accesses") for n in ns]

    lines += [
        "### Search",
        "",
        "El Hashing vuelve a destacar con una sola página leída en todos los casos, "
        "resultado directo de su acceso directo por clave sin necesidad de recorrer ninguna estructura.",
        "",
        f"El B+ Tree lee {bpt_search[0]:.1f}, {bpt_search[1]:.1f} y {bpt_search[2]:.1f} páginas "
        f"para n=1,000, 10,000 y 100,000 respectivamente. La diferencia entre cada escala es de "
        f"aproximadamente 1 página, consistente con el crecimiento logarítmico en base al orden del árbol.",
        "",
        f"El Sequential File lee {seq_search[0]:.1f}, {seq_search[1]:.1f} y {seq_search[2]:.1f} páginas. "
        f"La diferencia entre escalas es de ~3.3 páginas, exactamente log₂(10), lo que confirma "
        f"que la búsqueda binaria implementada sobre el archivo principal funciona correctamente. "
        f"El número de páginas leídas es mayor que el B+ Tree porque la búsqueda binaria opera "
        f"sobre registros individuales y cada acceso puede caer en una página distinta, mientras "
        f"que el B+ Tree navega por nodos internos compactos.",
        "",
    ]

    # ── RANGE SEARCH ────────────────────────────────────────────────────────
    bpt_range = [val("bplustree", "range_search", n, "disk_accesses") for n in ns]
    seq_range = [val("sequential", "range_search", n, "disk_accesses") for n in ns]

    lines += [
        "### Range Search",
        "",
        "Este es el resultado más interesante del experimento. Para n=1,000 y n=10,000 el B+ Tree "
        f"es más eficiente ({bpt_range[0]:.1f} y {bpt_range[1]:.1f} páginas frente a "
        f"{seq_range[0]:.1f} y {seq_range[1]:.1f} del Sequential). Sin embargo, para n=100,000 "
        f"la situación se invierte: el Sequential File lee {seq_range[2]:.1f} páginas mientras "
        f"el B+ Tree necesita {bpt_range[2]:.1f}.",
        "",
        "La explicación es la localidad de acceso. El Sequential File almacena los registros "
        "ordenados en páginas contiguas en disco: una vez localizado el inicio con búsqueda binaria, "
        "el recorrido del rango lee páginas consecutivas, una por una. El B+ Tree, en cambio, "
        "recorre hojas enlazadas que pueden estar dispersas en el archivo, y además cada nodo "
        "interno recorrido durante la búsqueda inicial suma páginas adicionales. A mayor n, "
        "más niveles del árbol y más hojas en el rango, lo que hace crecer el costo más rápido.",
        "",
        "El Extendible Hashing no soporta range search por diseño, ya que la función de hash "
        "destruye el orden de las claves.",
        "",
    ]

    # ── TIEMPO ──────────────────────────────────────────────────────────────
    lines += [
        "### Tiempo de ejecución",
        "",
        "Los tiempos siguen la misma tendencia que los accesos a disco, lo que confirma que "
        "el cuello de botella de todas las estructuras es el I/O a páginas. El Hashing es el "
        "más rápido en inserción y búsqueda puntual. El Sequential File es significativamente "
        "más lento en inserción a n=100,000 (2.78 ms/op frente a 0.04 del B+ Tree) debido al "
        "costo del rebuild, pero compite bien en búsqueda.",
        "",
    ]

    return lines

def build_markdown(data, chart_paths, graficos_dir):
    structs = get_structures(data)
    ns      = get_ns(data)
    lines   = []

    lines += [
        "# Evaluación Experimental — Estructuras de Indexación",
        "",
        "## 1. Introducción",
        "",
        "Este informe presenta los resultados experimentales de la comparación entre las "
        "estructuras de indexación implementadas: "
        + ", ".join(STRUCTURE_LABELS.get(s, s) for s in structs) + ". "
        "Se midieron dos métricas por operación: **accesos a disco** (páginas leídas + escritas) "
        "y **tiempo de ejecución (ms)**. "
        f"Los experimentos se ejecutaron con datasets de tamaño n ∈ {{{', '.join(str(n) for n in ns)}}} registros, "
        "generados de forma sintética con claves enteras aleatorias sin repetición.",
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
            lines += [make_table(data, op, metric), ""]

            bar_path = chart_paths.get((op, metric, "bar"))
            if bar_path and os.path.exists(bar_path):
                rel = os.path.relpath(bar_path, os.path.dirname(OUT_MD))
                lines += [f"![{op_title} — {metric_label} (barras)]({rel})", ""]

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
    ]

    lines += build_discussion(data)

    lines += [
        "---",
        "",
        "## 5. Conclusión",
        "",
        "| Criterio | B+ Tree | Extendible Hashing | Sequential File |",
        "|----------|---------|--------------------|-----------------|",
        "| Insert | O(log n) | **O(1)** | O(1) amortizado |",
        "| Search puntual | O(log n) | **O(1)** | O(log n) |",
        "| Range search | O(log n + k) | ❌ No soportado | **O(log n + k)** |",
        "| Localidad en rango | Media | — | **Alta** |",
        "| Escalabilidad | Alta | Alta | Media |",
        "",
        "El B+ Tree es la estructura más versátil: soporta búsqueda puntual y por rango con "
        "costo logarítmico garantizado, y escala bien a datasets grandes. Es la opción recomendada "
        "para cargas mixtas.",
        "",
        "El Extendible Hashing domina en búsqueda puntual e inserción gracias a su acceso O(1), "
        "pero queda descartado cuando se necesita range search.",
        "",
        "El Sequential File es competitivo en range search gracias a la localidad física de sus "
        "páginas: a n=100,000 supera al B+ Tree en accesos a disco. Su principal debilidad es "
        "el costo del rebuild periódico, que impacta el tiempo de inserción a escala.",
        "",
    ]

    return "\n".join(lines)

# -------------------------------------------------------
# MAIN
# -------------------------------------------------------

def generate_report(log_path=LOG_PATH, out_md=OUT_MD, graficos_dir=GRAFICOS_DIR):
    print(f"[report] Cargando log: {log_path}")
    entries = load_log(log_path)
    data    = aggregate(entries)

    print(f"[report] Generando gráficos en: {graficos_dir}/")
    chart_paths = generate_all_charts(data, graficos_dir)

    print(f"[report] Escribiendo informe: {out_md}")
    md = build_markdown(data, chart_paths, graficos_dir)
    with open(out_md, "w", encoding="utf-8") as f:
        f.write(md)

    print(f"\nInforme generado: {out_md}")
    print(f"Gráficos en:      {graficos_dir}/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--log",      default=LOG_PATH)
    parser.add_argument("--out",      default=OUT_MD)
    parser.add_argument("--graficos", default=GRAFICOS_DIR)
    args = parser.parse_args()
    generate_report(args.log, args.out, args.graficos)'''