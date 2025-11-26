# app.py
from flask import Flask, render_template, request
import pandas as pd
from pathlib import Path

app = Flask(__name__)

DATA_DIR = Path(__file__).parent / "data"

# Cache simple en memoria
_df_global = None


def load_data():
    global _df_global
    if _df_global is not None:
        return _df_global

    csv_files = list(DATA_DIR.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No se encontraron CSV en {DATA_DIR}")

    df_list = []
    for f in csv_files:
        print(f"Cargando: {f.name}")
        df = pd.read_csv(
            f,
            low_memory=False,
            dtype=str,  # todo como texto para evitar quilombos de tipos
        )
        df_list.append(df)

    df = pd.concat(df_list, ignore_index=True)

    # Parseo de fechas
    for col in ["time_from", "time_to", "date_ops_start", "date_ops_end"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Orden lógico
    sort_cols = [c for c in ["well_legal_name", "event_id", "time_from"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols)

    # Quedarnos con columnas interesantes
    wanted_cols = [
        "rig_name",
        "loc_fed_lease_no",
        "well_legal_name",
        "activity_class_desc",
        "activity_code_desc",
        "activity_duration",
        "expr1",
        "activity_subcode2",
        "step_no",
        "time_from",
        "time_to",
        "date_ops_end",
        "date_ops_start",
        "event_code",
        "event_objective_1",
        "event_objective_2",
        "event_id",
    ]

    cols_present = [c for c in wanted_cols if c in df.columns]
    df = df[cols_present]

    _df_global = df
    return df


@app.route("/", methods=["GET"])
def index():
    df = load_data()

    # Lista de pozos
    if "well_legal_name" not in df.columns or "event_id" not in df.columns:
        return "Faltan columnas 'well_legal_name' o 'event_id' en los CSV.", 500

    pozos = sorted(df["well_legal_name"].dropna().unique().tolist())

    # Lectura de parámetros
    pozo_sel = request.args.get("well")
    evento_sel = request.args.get("event")

    eventos = []
    df_evento = None

    if pozo_sel:
        df_pozo = df[df["well_legal_name"] == pozo_sel]

        # Resumen de eventos por pozo
        eventos_df = (
            df_pozo.groupby("event_id")
            .agg(
                date_ops_start=("date_ops_start", "min"),
                date_ops_end=("date_ops_end", "max"),
                event_objective_1=("event_objective_1", "first"),
            )
            .reset_index()
        )

        def fmt_fecha(x):
            if pd.isna(x):
                return "s/f"
            try:
                return x.date().isoformat()
            except Exception:
                return str(x)

        eventos = []
        for _, row in eventos_df.iterrows():
            eventos.append(
                {
                    "event_id": row["event_id"],
                    "label": f"{row['event_id']} | {fmt_fecha(row['date_ops_start'])} → {fmt_fecha(row['date_ops_end'])} | {row['event_objective_1']}",
                }
            )

        # Si hay un evento seleccionado, filtrar detalle
        if evento_sel:
            df_evento = df_pozo[df_pozo["event_id"] == evento_sel].copy()
            if "time_from" in df_evento.columns:
                df_evento = df_evento.sort_values("time_from")

    # Definir columnas a mostrar en tabla
    mostrar_cols = [
        "step_no",
        "time_from",
        "time_to",
        "rig_name",
        "loc_fed_lease_no",
        "well_legal_name",
        "activity_class_desc",
        "activity_code_desc",
        "activity_duration",
        "expr1",
        "activity_subcode2",
        "date_ops_start",
        "date_ops_end",
        "event_code",
        "event_objective_1",
        "event_objective_2",
    ]
    if df_evento is not None:
        mostrar_cols = [c for c in mostrar_cols if c in df_evento.columns]
        tabla_evento = df_evento[mostrar_cols].to_dict(orient="records")
    else:
        tabla_evento = None

    return render_template(
        "index.html",
        pozos=pozos,
        pozo_sel=pozo_sel,
        eventos=eventos,
        evento_sel=evento_sel,
        tabla_evento=tabla_evento,
        columnas=mostrar_cols,
    )


if __name__ == "__main__":
    # Para pruebas locales
    app.run(host="0.0.0.0", port=8080, debug=True)
