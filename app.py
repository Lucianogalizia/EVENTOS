# app.py
import pandas as pd
import streamlit as st
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"


@st.cache_data
def load_data():
    """Carga y combina todos los CSV de la carpeta data/."""
    csv_files = list(DATA_DIR.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No se encontraron CSV en {DATA_DIR}")

    df_list = []
    for f in csv_files:
        st.write(f"Cargando: {f.name}")
        df = pd.read_csv(
            f,
            low_memory=False,
            dtype=str,  # leemos todo como texto para evitar problemas
        )
        df_list.append(df)

    df = pd.concat(df_list, ignore_index=True)

    # Parseamos fechas/horas necesarias (si no existen, las ignoramos)
    for col in ["time_from", "time_to", "date_ops_start", "date_ops_end"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Orden cronológico por pozo + evento + desde
    sort_cols = []
    for col in ["well_legal_name", "event_id", "time_from"]:
        if col in df.columns:
            sort_cols.append(col)

    if sort_cols:
        df = df.sort_values(sort_cols)

    # Nos quedamos solo con las columnas que nos interesan, si existen
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
        "event_id",  # la necesitamos para identificar el evento
    ]

    cols_present = [c for c in wanted_cols if c in df.columns]
    df = df[cols_present]

    return df


def main():
    st.set_page_config(page_title="Intervenciones de pozos", layout="wide")
    st.title("Historial de intervenciones de pozos")

    df = load_data()

    if "well_legal_name" not in df.columns:
        st.error("No se encontró la columna 'well_legal_name' en los CSV.")
        st.stop()
    if "event_id" not in df.columns:
        st.error("No se encontró la columna 'event_id' en los CSV.")
        st.stop()

    # 1) Seleccionar pozo
    pozos = df["well_legal_name"].dropna().unique()
    pozo_sel = st.selectbox("Seleccioná pozo", sorted(pozos))

    df_pozo = df[df["well_legal_name"] == pozo_sel]

    # 2) Armar listado de eventos para ese pozo
    # Tomamos una fila representativa por event_id
    eventos = (
        df_pozo.groupby("event_id")
        .agg(
            date_ops_start=("date_ops_start", "min"),
            date_ops_end=("date_ops_end", "max"),
            event_objective_1=("event_objective_1", "first"),
        )
        .reset_index()
    )

    # Preparamos etiqueta legible
    def fmt_fecha(x):
        if pd.isna(x):
            return "s/f"
        try:
            return x.date().isoformat()
        except Exception:
            return str(x)

    eventos["label"] = eventos.apply(
        lambda r: f"{r['event_id']} | {fmt_fecha(r['date_ops_start'])} → {fmt_fecha(r['date_ops_end'])} | {r['event_objective_1']}",
        axis=1,
    )

    evento_label = st.selectbox("Seleccioná evento", eventos["label"].tolist())

    evento_sel = eventos.loc[eventos["label"] == evento_label, "event_id"].iloc[0]

    # 3) Detalle cronológico del evento
    df_evento = df_pozo[df_pozo["event_id"] == evento_sel].copy()

    if "time_from" in df_evento.columns:
        df_evento = df_evento.sort_values("time_from")

    st.subheader("Detalle cronológico de la intervención seleccionada")

    # Reordenamos columnas para mostrarlas prolijas
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
    mostrar_cols = [c for c in mostrar_cols if c in df_evento.columns]

    st.dataframe(df_evento[mostrar_cols], use_container_width=True)


if __name__ == "__main__":
    main()
