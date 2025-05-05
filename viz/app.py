# app.py

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta

# --- Config de la page (doit être le 1er appel Streamlit) ---
st.set_page_config(page_title="📊 Dashboard Yellow Taxi", layout="wide")

# Connexion
DB_URL = "postgresql://postgres:admin@localhost:15435/datamart"
engine = create_engine(DB_URL)

@st.cache_data(ttl=10*60)
def load_metadata():
    # récupère la liste des vendors et la plage de dates dispo
    vendors = pd.read_sql("SELECT DISTINCT vendor_id FROM dim_vendor", engine)["vendor_id"].tolist()
    dates = pd.read_sql("SELECT MIN(pickup_datetime) AS min_dt, MAX(pickup_datetime) AS max_dt FROM dim_time", engine)
    min_dt = dates["min_dt"].iloc[0]
    max_dt = dates["max_dt"].iloc[0]
    return vendors, min_dt, max_dt

vendors, min_dt, max_dt = load_metadata()

# --- Sidebar : filtres ---
st.sidebar.header("Filtres")
sel_vendors = st.sidebar.multiselect(
    "Vendor(s)", vendors, default=vendors
)
start_date, end_date = st.sidebar.date_input(
    "Plage de dates",
    [min_dt.date(), max_dt.date()],
    min_value=min_dt.date(),
    max_value=max_dt.date()
)
# convertir en timestamps
start_ts = datetime.combine(start_date, datetime.min.time())
end_ts   = datetime.combine(end_date,   datetime.max.time())

# --- Chargement des KPIs agrégés ---
@st.cache_data(ttl=5*60)
def load_kpis(vendors, start_ts, end_ts):
    q = f"""
    SELECT
      COUNT(*) AS trip_count,
      SUM(total_amount) AS revenue,
      AVG(total_amount) AS avg_fare,
      AVG(NULLIF(fare_amount,0)) AS avg_distance  -- remplacez par trip_distance si dispo
    FROM fact_trip f
      JOIN dim_time dt ON f.time_id = dt.id
    WHERE dt.pickup_datetime BETWEEN %(start)s AND %(end)s
      AND f.vendor_id = ANY(%(vendors)s)
    """
    return pd.read_sql(q, engine, params={
        "start": start_ts, "end": end_ts, "vendors": sel_vendors
    }).iloc[0]

kpis = load_kpis(sel_vendors, start_ts, end_ts)

# --- Header & KPIs ---
st.title("📊 Aperçu des trajets jaunes")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Nombre de trajets", f"{int(kpis.trip_count):,}")
col2.metric("Chiffre d’affaires", f"${kpis.revenue:,.2f}")
col3.metric("Ticket moyen", f"${kpis.avg_fare:,.2f}")
col4.metric("Distance moyenne", f"{kpis.avg_distance:.2f} km")

st.markdown("---")

# --- Graphiques complémentaires ---
# 1) Évolution horaire
@st.cache_data(ttl=5*60)
def hourly_distribution(vendors, start_ts, end_ts):
    q = f"""
    SELECT
      EXTRACT(HOUR FROM dt.pickup_datetime)::INT AS hr,
      COUNT(*) AS trips
    FROM fact_trip f
      JOIN dim_time dt ON f.time_id = dt.id
    WHERE dt.pickup_datetime BETWEEN %(start)s AND %(end)s
      AND f.vendor_id = ANY(%(vendors)s)
    GROUP BY hr ORDER BY hr
    """
    return pd.read_sql(q, engine, params={
        "start": start_ts, "end": end_ts, "vendors": sel_vendors
    }).set_index("hr")

hourly = hourly_distribution(sel_vendors, start_ts, end_ts)
st.subheader("🕐 Répartition horaire des trajets")
st.bar_chart(hourly)

# 2) Top 10 PULocationID
@st.cache_data(ttl=5*60)
def top_locations(vendors, start_ts, end_ts, limit=10):
    q = f"""
    SELECT dl.pu_location_id AS loc, COUNT(*) AS trips
    FROM fact_trip f
      JOIN dim_time dt ON f.time_id = dt.id
      JOIN dim_location dl ON f.location_id = dl.id
    WHERE dt.pickup_datetime BETWEEN %(start)s AND %(end)s
      AND f.vendor_id = ANY(%(vendors)s)
    GROUP BY loc ORDER BY trips DESC LIMIT %(lim)s
    """
    return pd.read_sql(q, engine, params={
        "start": start_ts, "end": end_ts, "vendors": sel_vendors, "lim": limit
    }).set_index("loc")

top10loc = top_locations(sel_vendors, start_ts, end_ts)
st.subheader("📍 Top 10 des zones de prise en charge")
st.bar_chart(top10loc)

# 3) Répartition par type de paiement
@st.cache_data(ttl=5*60)
def payment_distribution(vendors, start_ts, end_ts):
    q = f"""
    SELECT dpt.payment_type AS pay, COUNT(*) AS cnt
    FROM fact_trip f
      JOIN dim_time dt ON f.time_id = dt.id
      JOIN dim_payment_type dpt ON f.payment_type_id = dpt.id
    WHERE dt.pickup_datetime BETWEEN %(start)s AND %(end)s
      AND f.vendor_id = ANY(%(vendors)s)
    GROUP BY pay ORDER BY cnt DESC
    """
    return pd.read_sql(q, engine, params={
        "start": start_ts, "end": end_ts, "vendors": sel_vendors
    }).set_index("pay")

pay_dist = payment_distribution(sel_vendors, start_ts, end_ts)
st.subheader("💳 Répartition des types de paiement")
st.bar_chart(pay_dist)

# 4) Données brutes optionnelles
with st.expander("Voir les 100 premières lignes de fact_trip filtrées"):
    df_raw = pd.read_sql(f"""
      SELECT f.*, dt.pickup_datetime
      FROM fact_trip f
        JOIN dim_time dt ON f.time_id = dt.id
      WHERE dt.pickup_datetime BETWEEN %(start)s AND %(end)s
        AND f.vendor_id = ANY(%(vendors)s)
      LIMIT 100
    """, engine, params={
        "start": start_ts, "end": end_ts, "vendors": sel_vendors
    })
    st.dataframe(df_raw, use_container_width=True)
