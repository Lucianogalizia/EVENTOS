from flask import Flask, render_template, request
import pandas as pd
from pathlib import Path

app = Flask(__name__)

DATA_DIR = Path(__file__).parent / "data"

# Columnas que realmente usás en la app
WANTED_COLS = [
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
    "event_id",  # opcional, solo para mostrar si existe
]

# Índice liviano en memoria: 1 fila por (well, objective, code, date_ops_start)
_INDEX_DF = None


def build_index(chunksize: int = 50_000) -> pd.DataFrame:
    """
    Recorre TODOS los CSV en /data en chunks y arma un índice liviano
    donde cada evento se define por:
      well_legal_name + event_objective_1 + event_code + date_ops_start

    Guarda:
      - well_legal_name
      - event_objective_1
      - event_code
      - date_ops_start
      - date_ops_end (máx)
      - event_id (primero que aparezca, si existe)
      - filename(s) donde aparece el evento
    """
    global _INDEX_DF
    if _INDEX_DF is not None:
        return _INDEX_DF

    csv_files = list(DATA_DIR.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No se encontraron CSV en {DATA_DIR}")

    rows = []

    for f in csv_files:
        print(f"[INDEX] Procesando archivo para índice: {f.name}")

        for chunk in pd.read_csv(
            f,
            low_memory=False,
            dtype=str,
            chunksize=chunksize,
        ):
            # Nos quedamos solo con columnas relevantes
            cols_present = [c for c in WANTED_COLS if c in chunk.columns]
            chunk = chunk[cols_present]

            # Necesitamos al menos well + objective + code + date_ops_start
            required = ["well_legal_name", "event_objective_1", "event_code", "date_ops_start"]
            if not all(col in chunk.columns for col in required):
                continue

            # Parseo liviano de fechas
            for col in ["date_ops_start", "date_ops_end"]:
                if col in chunk.columns:
                    chunk[col] = pd.to_datetime(chunk[col], errors="coerce")

            # Agrupamos por well + objetivo + código + fecha inicio
            grp_cols = ["well_legal_name", "event_objective_1", "event_code", "date_ops_start"]

            agg_dict = {
                "date_ops_end": ("date_ops_end", "max"),
            }
            if "event_id" in chunk.columns:
                agg_dict["event_id"] = ("event_id", "first")

            resumen = (
                chunk.groupby(grp_cols)
                .agg(**agg_dict)
                .reset_index()
            )

            if resumen.empty:
                continue

            # Guardamos nombre de archivo para saber dónde buscar el detalle
            resumen["filename"] = f.name
            rows.append(resumen)

    if not rows:
        raise RuntimeError("No se pudo construir índice; no se encontraron eventos válidos.")

    idx = pd.concat(rows, ignore_index=True)

    # Función para unificar archivos
    def collect_files(series):
        return sorted(set(series.dropna().tolist()))

    group_cols = ["well_legal_name", "event_objective_1", "event_code", "date_ops_start"]
    agg_final = {
        "date_ops_end": ("date_ops_end", "max"),
        "filename": ("filename", collect_files),
    }
    if "event_id" in idx.columns:
        agg_final["event_id"] = ("event_id", "first")

    index_df = (
        idx.groupby(group_cols)
        .agg(**agg_final)
        .reset_index()
    )

    # Aseguramos tipo datetime
    for col in ["date_ops_start", "date_ops_end"]:
        if col in index_df.columns:
            index_df[col] = pd.to_datetime(index_df[col], errors="coerce")

    # ID interno para usar en el combo de eventos
    index_df = index_df.reset_index(drop=True)
    index_df["idx_id"] = index_df.index.astype(str)

    _INDEX_DF = index_df
    print("[INDEX] Construcción de índice completa.")
    return index_df


def load_event_detail(
    well: str,
    event_objective_1: str | None,
    event_code: str | None,
    date_start,
    filenames,
    chunksize: int = 50_000,
) -> pd.DataFrame:
    """
    Carga SOLO las filas correspondientes a:
      well_legal_name == well
      event_objective_1 == event_objective_1 (si existe)
      event_code == event_code (si existe)
      date_ops_start == date_start (mismo día)

    recorriendo en chunks los archivos donde sabemos que aparece.
    """
    if isinstance(filenames, str):
        filenames = [filenames]

    rows = []

    # Guardamos el día de inicio como date (para comparar por día)
    date_start_date = None
    if pd.notna(date_start):
        try:
            date_start_date = pd.to_datetime(date_start).date()
        except Exception:
            date_start_date = None

    for fname in filenames:
        path = DATA_DIR / fname
        if not path.exists():
            print(f"[DETAIL] Archivo no encontrado: {path}")
            continue

        print(f"[DETAIL] Buscando detalle en: {fname}")
        for chunk in pd.read_csv(
            path,
            low_memory=False,
            dtype=str,
            chunksize=chunksize,
        ):
            cols_present = [c for c in WANTED_COLS if c in chunk.columns]
            chunk = chunk[cols_present]

            if "well_legal_name" not in chunk.columns:
                continue

            mask = (chunk["well_legal_name"] == well)

            if event_objective_1 is not None and "event_objective_1" in chunk.columns:
                mask &= (chunk["event_objective_1"] == event_objective_1)

            if event_code is not None and "event_code" in chunk.columns:
                mask &= (chunk["event_code"] == event_code)

            # Filtrar también por fecha de inicio (por día)
            if date_start_date is not None and "date_ops_start" in chunk.columns:
                chunk["date_ops_start"] = pd.to_datetime(
                    chunk["date_ops_start"], errors="coerce"
                )
                mask &= chunk["date_ops_start"].dt.date.eq(date_start_date)

            sub = chunk.loc[mask]
            if sub.empty:
                continue

            rows.append(sub)

    if not rows:
        # Devuelve DF vacío con las columnas esperadas
        return pd.DataFrame(columns=WANTED_COLS)

    df = pd.concat(rows, ignore_index=True)

    # Parseo de fechas para orden y display
    for col in ["time_from", "time_to", "date_ops_start", "date_ops_end"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Orden lógico por hora de inicio y paso
    sort_cols = [c for c in ["time_from", "step_no"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols)

    return df


@app.route("/", methods=["GET"])
def index():
    # 1) Construimos / obtenemos índice liviano
    index_df = build_index()

    # Lista de pozos desde el índice
    pozos = sorted(index_df["well_legal_name"].dropna().unique().tolist())

    # Lectura de parámetros
    pozo_sel = request.args.get("well")
    evento_sel = request.args.get("event")  # acá llega idx_id (string)

    eventos = []
    tabla_evento = None
    columnas = []

    if pozo_sel:
        # 2) Eventos de ese pozo
        eventos_df = index_df[index_df["well_legal_name"] == pozo_sel].copy()

        # Orden: del evento más reciente al más antiguo
        if "date_ops_end" in eventos_df.columns:
            eventos_df = eventos_df.sort_values(
                "date_ops_end", ascending=False, na_position="last"
            )

        def fmt_fecha(x):
            if pd.isna(x):
                return "s/f"
            try:
                return x.date().isoformat()
            except Exception:
                return str(x)

        for _, row in eventos_df.iterrows():
            label = (
                f"{row.get('event_code', '')} | "
                f"{fmt_fecha(row.get('date_ops_start'))} → {fmt_fecha(row.get('date_ops_end'))} | "
                f"{row.get('event_objective_1', '')}"
            )
            eventos.append(
                {
                    "event_id": row["idx_id"],  # usamos idx_id como valor del combo
                    "label": label,
                }
            )

        # 3) Si hay evento seleccionado, cargamos el detalle a demanda
        if evento_sel:
            fila_evt = eventos_df[eventos_df["idx_id"] == evento_sel]
            if not fila_evt.empty:
                fila_evt = fila_evt.iloc[0]

                files = fila_evt["filename"]
                evt_obj1 = fila_evt.get("event_objective_1", None)
                evt_code = fila_evt.get("event_code", None)
                date_start = fila_evt.get("date_ops_start", None)

                df_detalle = load_event_detail(
                    pozo_sel,
                    evt_obj1,
                    evt_code,
                    date_start,
                    files,
                )

                # Columnas a mostrar (las que existan)
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
                    "event_id",
                ]
                columnas = [c for c in columnas if c in df_detalle.columns]

                # Pasamos a lista de dicts para el template
                tabla_evento = df_detalle[columnas].to_dict(orient="records")

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
    # Para pruebas locales
    app.run(host="0.0.0.0", port=8080, debug=True)





