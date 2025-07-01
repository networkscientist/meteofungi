import streamlit as st
from datetime import datetime, timedelta
try:
    raise ImportError
    import polars as pl
except ImportError:
    pl = None
    import pandas as pd
# --- Load data ---
st.set_page_config(layout='wide')


@st.cache_data
def load_rainfall():
    if pl:
        return pl.read_parquet('rainfall.parquet')
    else:
        return pd.read_parquet('rainfall.parquet')


@st.cache_data
def load_metrics():
    if pl:
        return pl.read_parquet('metrics.parquet')
    else:
        return pd.read_parquet('metrics.parquet')


# def load_rainfall_from_local(stations_rainfall, metadata):
#     return load_rainfall(stations_rainfall, metadata['precipitation'], from_local=True)

# meta = load_metadata()
rainfall = load_rainfall()
metrics = load_metrics()
st.title('MeteoFungi')
st.area_chart(data=(rainfall.filter(pl.col('Time') >= (datetime.now() - timedelta(days=7))) if pl else rainfall.loc[rainfall.Time >= (datetime.now() - timedelta(days=7))]), x='Time', y='Rainfall', color='Station', x_label='Time', y_label='Rainfall (mm)')

st.subheader('3-Day Rainfall Sum (mm)')
a, b, c, d, e = st.columns(5)
for col, station in zip([a, b, c, d, e], (sorted(metrics.unique(subset=['Station']).get_column('Station').to_list()) if pl else rainfall.Station.unique())):
    val = round((metrics.filter((pl.col('Station')==station) & (pl.col('aggr_period_days')==7)).select(pl.col('Rainfall')).item() if pl else metrics.loc[(metrics.Station == station) & (metrics.aggr_period_days == 7), 'Rainfall'].iloc[0]), 2)
    if val < 5:
        emo = '☀️'
    elif (val >= 5) & (val < 10):
        emo = '🌦️'
    elif val >= 10:
        emo = '🌧️'
    else:
        emo = '🌊'
    col.metric(label=station, value=str(val) + emo)
st.info('Source: MeteoSwiss')
