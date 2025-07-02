import streamlit as st
from datetime import datetime, timedelta

try:
    # raise ImportError
    import polars as pl
except ImportError:
    pl = None
    import pandas as pd
# --- Load data ---
st.set_page_config(layout='wide')


@st.cache_data
def load_rainfall():
    if pl:
        return pl.scan_parquet('rainfall.parquet')
    else:
        return pd.read_parquet('rainfall.parquet')


@st.cache_data
def load_metrics():
    if pl:
        return pl.read_parquet('metrics.parquet')
    else:
        return pd.read_parquet('metrics.parquet')


@st.cache_data
def load_meta_stations():
    if pl:
        # meta_precip = pl.read_csv('ogd-smn-precip_meta_stations.csv', encoding='ISO-8859-1', separator=';')
        # meta_weather = pl.read_csv('ogd-smn_meta_stations.csv', encoding='ISO-8859-1', separator=';', schema=meta_precip.schema)
        return pl.scan_parquet('meta_stations.parquet')
    else:
        pass


meta = load_meta_stations()
rainfall = (
    load_rainfall()
    .join(meta.select(['station_abbr', 'station_name']).rename({'station_abbr': 'Station'}), on=['Station'])
    .select(
        [  # Replace Station with station_name
            pl.exclude('Station')  # Select all other columns except 'Station'
        ]
    )
    .rename({'station_name': 'Station'})
)

metrics = load_metrics()
st.title('MeteoFungi')
st.area_chart(
    data=(
        rainfall.filter(pl.col('Time') >= (datetime.now() - timedelta(days=7)))
        if pl
        else rainfall.loc[rainfall.Time >= (datetime.now() - timedelta(days=7))]
    ),
    x='Time',
    y='Rainfall',
    color='Station',
    x_label='Time',
    y_label='Rainfall (mm)',
)

st.subheader('3-Day Rainfall Sum (mm)')
a, b, c, d, e = st.columns(5)
for col, station in zip(
    [a, b, c, d, e],
    (sorted(metrics.unique(subset=['Station']).get_column('Station').to_list()) if pl else rainfall.Station.unique()),
):
    val = round(
        (
            metrics.filter((pl.col('Station') == station) & (pl.col('aggr_period_days') == 7))
            .select(pl.col('Rainfall'))
            .item()
            if pl
            else metrics.loc[(metrics.Station == station) & (metrics.aggr_period_days == 7), 'Rainfall'].iloc[0]
        ),
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
