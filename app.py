from flask import Flask, render_template, request
import pandas as pd
from google.cloud import bigquery

app = Flask(__name__)

# ============================
# CONFIG BIGQUERY
# ============================
PROJECT_ID = "eventos-479403"
DATASET_TABLE = "eventos_pozos.eventos"   # dataset.tabla que creaste en BigQuery

bq_client = bigquery.Client(project=PROJECT_ID)

_df_global = None  # cache simple en memoria


def load_data():
    """Carga todos los eventos desde BigQuery una sola vez."""
    global _df_global
    if _df_global is not None:
        return _df_global

    query = f"""
        SELECT
            contractor_name,
            rig_name,
            loc_fed_lease_no,
            loc_region,
            site_name,
            field_name,
            well_legal_name,
            activity_class,
            activity_class_desc,
            activity_code,
            activity_code_desc,
            activity_duration,
            activity_group,
            expr1,
            activity_phase,
            billing_code,
            activity_subcode,
            activity_subcode2,
            cost_code,
            pickup_weight,
            step_no,
            time_from,
            time_to,
            date_ops_end,
            date_ops_start,
            event_code,
            event_id,
            event_objective_1,
            event_objective_2,
            event_type,
            status_end,
            well_id,
            date_time_off_location,
            date_report,
            entity_type,
            date_rig_pickup
        FROM `{PROJECT_ID}.{DATASET_TABLE}`
    """

    df = bq_client.query(query).to_dataframe()

    # Parseo de fechas
    for col in ["time_from", "time_to", "date_ops_start", "date_ops_end"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Orden lógico por pozo, evento, tiempo
    sort_cols = [c for c in ["well_legal_name", "event_id", "time_from"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols)

    # Columnas que realmente usa la app
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

    if "well_legal_name" not in df.columns or "event_id" not in df.columns:
        return "Faltan columnas 'well_legal_name' o 'event_id' en la tabla de BigQuery.", 500

    # Lista de pozos
    pozos = sorted(df["well_legal_name"].dropna().unique().tolist())

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

        # Ordenar eventos del más reciente al más antiguo
        if "date_ops_start" in eventos_df.columns:
            eventos_df = eventos_df.sort_values("date_ops_start", ascending=False)

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
                    "label": f"{row['event_id']} | "
                             f"{fmt_fecha(row['date_ops_start'])} → {fmt_fecha(row['date_ops_end'])} | "
                             f"{row['event_objective_1']}",
                }
            )

        # Detalle del evento seleccionado
        if evento_sel:
            df_evento = df_pozo[df_pozo["event_id"] == evento_sel].copy()
            if "time_from" in df_evento.columns:
                df_evento = df_evento.sort_values("time_from")

    # Columnas a mostrar en la tabla
    columnas = [
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
        columnas = [c for c in columnas if c in df_evento.columns]
        tabla_evento = df_evento[columnas].to_dict(orient="records")
    else:
        tabla_evento = None

    return render_template(
        "index.html",
        pozos=pozos,
        pozo_sel=pozo_sel,
        eventos=eventos,
        evento_sel=evento_sel,
        tabla_evento=tabla_evento,
        columnas=columnas,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)







