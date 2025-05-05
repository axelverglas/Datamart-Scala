# app.py

import streamlit as st
import pandas as pd

# ——————————————————————————————
# 1️⃣ CONFIGURATION
# ——————————————————————————————

# ⚠️ doit être le tout premier appel st.* !
st.set_page_config(
    page_title="📊 Dashboard Yellow Taxi",
    layout="wide"
)

st.title("📊 Aperçu global du Data Mart")

# Connexion SQL intégrée (sans passer par secrets.toml)
conn = st.connection(
    "postgresql", type="sql",
    dialect  = "postgresql",
    host     = "localhost",
    port     = 15435,
    database = "datamart",
    username     = "postgres",
    password = "admin"
)

# ——————————————————————————————
# 2️⃣ MÉTRIQUES GLOBALES
# ——————————————————————————————

@st.cache_data(ttl=600)
def load_overall_stats():
    q = """
    SELECT 
      COUNT(*)       AS total_trips,
      SUM(fare_amount) AS total_revenue,
      AVG(fare_amount) AS avg_fare
    FROM fact_trip;
    """
    return conn.query(q)

stats = load_overall_stats().iloc[0]
col1, col2, col3 = st.columns(3)
col1.metric("🚕 Nombre total de courses", f"{stats.total_trips:,}")
col2.metric("💰 Chiffre d'affaires total", f"${stats.total_revenue:,.2f}")
col3.metric("💵 Ticket moyen",      f"${stats.avg_fare:,.2f}")

st.markdown("---")

# ——————————————————————————————
# 3️⃣ DÉCOMPOSITION PAR ANNÉE
# ——————————————————————————————

@st.cache_data(ttl=600)
def load_revenue_by_year():
    q = """
    SELECT
      EXTRACT(YEAR FROM dt.pickup_datetime)::INT AS year,
      SUM(ft.fare_amount) AS revenue
    FROM fact_trip ft
    JOIN dim_time dt ON ft.time_id = dt.id
    GROUP BY year
    ORDER BY year;
    """
    return conn.query(q)

df_year = load_revenue_by_year().set_index("year")
st.subheader("📈 Chiffre d'affaires par année")
st.line_chart(df_year["revenue"])

# ——————————————————————————————
# 4️⃣ ACTIVITÉ PAR HEURE
# ——————————————————————————————

@st.cache_data(ttl=600)
def load_trips_by_hour():
    q = """
    SELECT
      EXTRACT(HOUR FROM dt.pickup_datetime)::INT AS hour,
      COUNT(*) AS trips
    FROM fact_trip ft
    JOIN dim_time dt ON ft.time_id = dt.id
    GROUP BY hour
    ORDER BY hour;
    """
    return conn.query(q)

df_hour = load_trips_by_hour().set_index("hour")
st.subheader("🕐 Nombre de courses par heure de la journée")
st.bar_chart(df_hour["trips"])

# ——————————————————————————————
# 5️⃣ CA PAR FOURNISSEUR
# ——————————————————————————————

@st.cache_data(ttl=600)
def load_revenue_by_vendor():
    q = """
    SELECT
      dv.vendor_id,
      SUM(ft.fare_amount) AS revenue
    FROM fact_trip ft
    JOIN dim_vendor dv ON ft.vendor_id = dv.id
    GROUP BY dv.vendor_id
    ORDER BY revenue DESC
    LIMIT 10;
    """
    return conn.query(q)

df_vendor = load_revenue_by_vendor().set_index("vendor_id")
st.subheader("🚖 Top 10 des vendors par CA")
st.bar_chart(df_vendor["revenue"])

# ——————————————————————————————
# 6️⃣ DISTRIBUTION PAR TYPE DE PAIEMENT
# ——————————————————————————————

@st.cache_data(ttl=600)
def load_by_payment():
    q = """
    SELECT
      dpt.payment_type,
      COUNT(*) AS count_trips
    FROM fact_trip ft
    JOIN dim_payment_type dpt ON ft.payment_type_id = dpt.id
    GROUP BY dpt.payment_type
    ORDER BY count_trips DESC;
    """
    return conn.query(q)

df_pay = load_by_payment().set_index("payment_type")
st.subheader("💳 Nombre de courses par type de paiement")
st.bar_chart(df_pay["count_trips"])

# ——————————————————————————————
# 7️⃣ DÉTAILS BRUTS
# ——————————————————————————————

with st.expander("🔎 Voir les 100 premières lignes de fact_trip"):
    df_raw = conn.query("SELECT * FROM fact_trip LIMIT 100")
    st.dataframe(df_raw, use_container_width=True)
