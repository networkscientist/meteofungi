import streamlit as st
from datetime import datetime, timedelta

import polars as pl

# --- Load data ---
st.set_page_config(layout='wide')


@st.cache_data
def load_rainfall():
    return pl.scan_parquet('rainfall.parquet').rename(
        {'reference_timestamp': 'Time', 'rre150h0': 'Rainfall', 'erefaoh0': 'Verdunstung', 'station_name': 'Station'}
    )


@st.cache_data
def load_metrics():
    return pl.scan_parquet('metrics.parquet').rename(
        {'rre150h0': 'Rainfall', 'erefaoh0':'Verdunstung', 'station_name': 'Station', 'aggr_period_days': 'Time Period'}
    )


@st.cache_data
def load_meta_stations():
    return pl.scan_parquet('meta_stations.parquet').rename({'station_abbr': 'Station'})


def get_rainfall_emoji(val):
    if val < 1:
        return '☀️'  # No rain
    elif 1 <= val < 10:
        return '🌦️'  # Light rain
    elif 10 <= val < 20:
        return '🌧️'  # Moderate rain
    elif 20 <= val < 50:
        return '🌊'  # Heavy rain
    else:
        return '🌧️🌊'  # Very heavy rain


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

st.subheader('3-Day Average (mm/d)')

a, b, c, d, e = st.columns(5)
for col, station in zip(
    [a, b, c, d, e],
    station_name_list,
):
    val = (
              (metrics.filter((pl.col('Station') == station) & (pl.col('Time Period') == 3))
        .select(pl.col('Rainfall'))
        .collect())
        .item() if (len(metrics.filter((pl.col('Station') == station) & (pl.col('Time Period') == 3)).select(pl.col('Rainfall')).collect()) > 0) else 0
        / 3
    )

    delta = val - (
        metrics.filter((pl.col('Station') == station) & (pl.col('Time Period') == 14))
        .select(pl.col('Rainfall'))
        .collect()
        .item()
        / 14
    )
    col.metric(label=station, value=(str(round(val, 1)) + ' ' + get_rainfall_emoji(val)), delta=round(delta, 1))

st.subheader('3-Day Average (mm/d)')

a, b, c, d, e = st.columns(5)
for col, station in zip(
    [a, b, c, d, e],
    station_name_list,
):
    val = (
        metrics.filter((pl.col('Station') == station) & (pl.col('Time Period') == 3))
        .select(pl.col('Verdunstung'))
        .collect()
        .item()
        / 3
    )

    delta = val - (
        metrics.filter((pl.col('Station') == station) & (pl.col('Time Period') == 14))
        .select(pl.col('Verdunstung'))
        .collect()
        .item()
        / 14
    )
    col.metric(label=station, value=(str(round(val, 1)) + ' ' + get_rainfall_emoji(val)), delta=round(delta, 1))

with st.expander('Further Information'):
    st.text('Delta values indicate difference between 3-day average and 14-day average.')
    st.info('Data Sources: MeteoSwiss')
