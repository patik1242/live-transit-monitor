"""Gdańsk Live Transit Monitor — Streamlit live map.

Reads the latest fleet positions from the gold table `gold_fleet_current`
(one row per vehicle) through the Databricks SQL connector and renders a
self-refreshing Folium map, coloured by delay bucket.

Runs locally (outside Databricks), so a non-Databricks audience can watch the
live fleet. Configuration comes from Streamlit secrets or environment variables
— never hard-code the token in this file.

Run:
    pip install -r requirements.txt
    streamlit run app.py
"""

import os

import folium
import pandas as pd
import streamlit as st
from streamlit_folium import st_folium
from streamlit_autorefresh import st_autorefresh
from databricks import sql

# --- optional .env support for local dev ---
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# --------------------------------------------------------------------------- #
# Configuration (from st.secrets first, then environment variables)
# --------------------------------------------------------------------------- #
def _cfg(name: str, default: str | None = None) -> str | None:
    if name in st.secrets:
        return st.secrets[name]
    return os.environ.get(name, default)


SERVER_HOSTNAME = _cfg("DATABRICKS_SERVER_HOSTNAME")
HTTP_PATH       = _cfg("DATABRICKS_HTTP_PATH")
ACCESS_TOKEN    = _cfg("DATABRICKS_TOKEN")
CATALOG         = _cfg("CATALOG", "dbr_dev")
SCHEMA          = _cfg("SCHEMA", "live_transit_monitor")
TABLE           = f"{CATALOG}.{SCHEMA}.gold_fleet_current"

REFRESH_SECONDS = 60
GDANSK_CENTER   = [54.372, 18.62]

# delay_bucket -> colour (matches the silver/gold buckets)
BUCKET_COLORS = {"early": "#eab308", "on_time": "#22c55e", "delayed": "#ef4444"}
BUCKET_LABELS = {"early": "Early", "on_time": "On time", "delayed": "Delayed"}


# --------------------------------------------------------------------------- #
# Data access
# --------------------------------------------------------------------------- #
@st.cache_data(ttl=REFRESH_SECONDS - 5, show_spinner=False)
def load_fleet() -> pd.DataFrame:
    """Fetch the current fleet snapshot from gold_fleet_current."""
    query = f"""
        SELECT vehicleCode, routeShortName, headsign,
               lat, lon, delay_min, delay_bucket, transportationType,
               event_time_local
        FROM {TABLE}
        WHERE lat IS NOT NULL AND lon IS NOT NULL
    """
    with sql.connect(server_hostname=SERVER_HOSTNAME,
                     http_path=HTTP_PATH,
                     access_token=ACCESS_TOKEN) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            df = cur.fetchall_arrow().to_pandas()
    return df


def build_map(df: pd.DataFrame) -> folium.Map:
    m = folium.Map(location=GDANSK_CENTER, zoom_start=12, tiles="CartoDB positron")
    for _, v in df.iterrows():
        color = BUCKET_COLORS.get(v.delay_bucket, "gray")
        popup = (f"<b>{v.routeShortName}</b> → {v.headsign}<br>"
                 f"{v.transportationType or ''} · {v.vehicleCode}<br>"
                 f"Delay: {v.delay_min} min")
        folium.CircleMarker(
            location=[v.lat, v.lon],
            radius=4, weight=0, fill=True, fill_opacity=0.85,
            fill_color=color,
            popup=folium.Popup(popup, max_width=250),
        ).add_to(m)
    return m


# --------------------------------------------------------------------------- #
# UI
# --------------------------------------------------------------------------- #
st.set_page_config(page_title="Gdańsk Live Transit Monitor", layout="wide")
st_autorefresh(interval=REFRESH_SECONDS * 1000, key="live")

st.title("Gdańsk Live Transit Monitor")

# Fail clearly if the connection settings are missing.
missing = [n for n, v in {
    "DATABRICKS_SERVER_HOSTNAME": SERVER_HOSTNAME,
    "DATABRICKS_HTTP_PATH": HTTP_PATH,
    "DATABRICKS_TOKEN": ACCESS_TOKEN,
}.items() if not v]
if missing:
    st.error(
        "Missing configuration: " + ", ".join(missing) +
        ".\n\nSet them in `.streamlit/secrets.toml` or as environment variables "
        "before running the app."
    )
    st.stop()

try:
    fleet = load_fleet()
except Exception as exc:  # noqa: BLE001 - surface any connection/query error to the user
    st.error(f"Could not load data from {TABLE}:\n\n{exc}")
    st.stop()

if fleet.empty:
    st.warning("No vehicles in gold_fleet_current yet — is the pipeline running and feeding gold?")
    st.stop()

# --- KPI row ---
counts = fleet["delay_bucket"].value_counts()
c1, c2, c3, c4 = st.columns(4)
c1.metric("Vehicles", len(fleet))
c2.metric("Delayed", int(counts.get("delayed", 0)))
c3.metric("On time", int(counts.get("on_time", 0)))
c4.metric("Early", int(counts.get("early", 0)))

latest = pd.to_datetime(fleet["event_time_local"]).max()
st.caption(f"Latest position: {latest}  ·  auto-refresh every {REFRESH_SECONDS}s")

# --- Map ---
st_folium(build_map(fleet), width=1200, height=650, returned_objects=[])
