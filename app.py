from flask import Flask, render_template, request
import pandas as pd
from google.cloud import bigquery

app = Flask(__name__)

# ============================
# CONFIG BIGQUERY
# ============================
PROJECT_ID = "eventos-479403"
DATASET_TABLE = "eventos_pozos.eventos_fix"   # dataset.tabla en BigQuery
TABLE_ID = f"{PROJECT_ID}.{DATASET_TABLE}"

bq_client = bigquery.Client(project=PROJECT_ID)

# -------------------------------------------------
# Helpers de consulta a BigQuery (sin traer todo)
# -------------------------------------------------
def get_pozos():
    """Devuelve lista de pozos (well_legal_name) distintos."""
    query = f"""
        SELECT DISTINCT well_legal_name
        FROM `{TABLE_ID}`
        WHERE well_legal_name IS NOT NULL
        ORDER BY well_legal_name
    """
    df = bq_client.query(query).to_dataframe()
    return df["well_legal_name"].tolist()


def get_eventos_de_pozo(pozo):
    """
    Devuelve un DataFrame con un evento por fila para el pozo:
      - event_id
      - date_ops_start (mínima)
      - date_ops_end   (máxima)
      - event_objective_1 (cualquiera)
    """
    query = f"""
        SELECT
            event_id,
            MIN(date_ops_start) AS date_ops_start,
            MAX(date_ops_end)   AS date_ops_end,
            ANY_VALUE(event_objective_1) AS event_objective_1
        FROM `{TABLE_ID}`
        WHERE well_legal_name = @pozo
        GROUP BY event_id
        ORDER BY date_ops_start DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("pozo", "STRING", pozo)
        ]
    )
    df = bq_client.query(query, job_config=job_config).to_dataframe()

    # Aseguramos tipos fecha
    for col in ["date_ops_start", "date_ops_end"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


def get_detalle_evento(pozo, event_id):
    """
    Devuelve el detalle de un evento (todas las filas / steps) para un pozo y event_id.
    """
    query = f"""
        SELECT
            step_no,
            time_from,
            time_to,
            rig_name,
            loc_fed_lease_no,
            well_legal_name,
            activity_class_desc,
            activity_code_desc,
            activity_duration,
            expr1,
            activity_subcode2,
            date_ops_start,
            date_ops_end,
            event_code,
            event_objective_1,
            event_objective_2
        FROM `{TABLE_ID}`
        WHERE well_legal_name = @pozo
          AND event_id       = @event_id
        ORDER BY time_from
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("pozo", "STRING", pozo),
            bigquery.ScalarQueryParameter("event_id", "STRING", event_id),
        ]
    )
    df = bq_client.query(query, job_config=job_config).to_dataframe()

    # Aseguramos fechas/horas como datetime
    for col in ["time_from", "time_to", "date_ops_start", "date_ops_end"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


# ============================
# RUTA PRINCIPAL
# ============================
@app.route("/", methods=["GET"])
def index():
    # Lista de pozos para el combo
    try:
        pozos = get_pozos()
    except Exception as e:
        return f"Error consultando pozos en BigQuery: {e}", 500

    pozo_sel = request.args.get("well")
    evento_sel = request.args.get("event")

    eventos = []
    df_evento = None

    if pozo_sel:
        # ---- Eventos del pozo seleccionado ----
        eventos_df = get_eventos_de_pozo(pozo_sel)

        # Construimos lista para el <select>
        def fmt_fecha(x):
            if pd.isna(x):
                return "s/f"
            try:
                return x.date().isoformat()
            except Exception:
                return str(x)

        for _, row in eventos_df.iterrows():
            eventos.append(
                {
                    "event_id": row["event_id"],
                    "label": f"{row['event_id']} | "
                             f"{fmt_fecha(row['date_ops_start'])} → {fmt_fecha(row['date_ops_end'])} | "
                             f"{row['event_objective_1']}",
                }
            )

        # ---- Validar que el evento seleccionado pertenezca al pozo ----
        eventos_ids_pozo = set(eventos_df["event_id"].astype(str))
        if evento_sel and evento_sel not in eventos_ids_pozo:
            # Si venía de otro pozo, lo ignoramos para que no quede un evento “fantasma”
            evento_sel = None

        # ---- Detalle del evento (si hay evento válido) ----
        if evento_sel:
            df_evento = get_detalle_evento(pozo_sel, evento_sel)

    # Columnas a mostrar en la tabla (mismo orden que ya usabas)
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

    if df_evento is not None and not df_evento.empty:
        # Solo mostramos columnas que realmente existen en el DF
        columnas_presentes = [c for c in columnas if c in df_evento.columns]
        tabla_evento = df_evento[columnas_presentes].to_dict(orient="records")
    else:
        columnas_presentes = columnas  # para que Jinja no rompa
        tabla_evento = None

    return render_template(
        "index.html",
        pozos=pozos,
        pozo_sel=pozo_sel,
        eventos=eventos,
        evento_sel=evento_sel,
        tabla_evento=tabla_evento,
        columnas=columnas_presentes,
    )


if __name__ == "__main__":
    # Para correr local
    app.run(host="0.0.0.0", port=8080, debug=True)








