import streamlit as st
from datetime import datetime, timedelta

import polars as pl

# --- Load data ---
st.set_page_config(layout='wide')


@st.cache_data
def load_rainfall():
    return pl.scan_parquet('rainfall.parquet').rename(
        {'reference_timestamp': 'Time', 'rre150h0': 'Rainfall', 'station_name': 'Station'}
    )


@st.cache_data
def load_metrics():
    return pl.scan_parquet('metrics.parquet').rename(
        {'rre150h0': 'Rainfall', 'station_name': 'Station', 'aggr_period_days': 'Time Period'}
    )


@st.cache_data
def load_meta_stations():
    return pl.scan_parquet('meta_stations.parquet').rename({'station_abbr': 'Station'})


meta = load_meta_stations()
rainfall = load_rainfall()

metrics = load_metrics()
st.title('MeteoFungi')
st.area_chart(
    data=rainfall.filter(pl.col('Time') >= (datetime.now() - timedelta(days=7))),
    x='Time',
    y='Rainfall',
    color='Station',
    x_label='Time',
    y_label='Rainfall (mm)',
)

station_name_list = metrics.unique(subset=['Station']).sort('Station').select('Station').collect().to_series().to_list()

st.subheader('3-Day Rainfall Sum (mm)')
a, b, c, d, e = st.columns(5)
for col, station in zip(
    [a, b, c, d, e],
    station_name_list,
):
    val = round(
        metrics.filter((pl.col('Station') == station) & (pl.col('Time Period') == 7))
        .select(pl.col('Rainfall'))
        .collect()
        .item(),
        2,
    )
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
