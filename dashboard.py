import streamlit as st
from datetime import datetime, timedelta

import polars as pl

# --- Load data ---
st.set_page_config(layout='wide', initial_sidebar_state='expanded')


@st.cache_data
def load_rainfall():
    return pl.scan_parquet('rainfall.parquet').rename(
        {
            'reference_timestamp': 'Time',
            'rre150h0': 'Rainfall',
            'station_name': 'Station',
        }
    )


time_periods = {period: (datetime.now() - timedelta(days=period)) for period in [3, 7, 14, 30]}


@st.cache_data
def create_metrics(_df, time_periods):
    return pl.concat(
        [
            _df.filter(pl.col('Time') >= datetime_period)
            .drop('Time')
            .group_by(['station_abbr', 'Station'])
            .agg(pl.sum('Rainfall'))
            .with_columns(pl.lit(period).alias('Time Period'))
            for period, datetime_period in time_periods.items()
        ]
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

metrics = create_metrics(rainfall, time_periods)
st.title('MeteoFungi')
st.area_chart(
    data=(
        rainfall.sort('Time')
        .filter(pl.col('Time') >= (datetime.now() - timedelta(days=7)))
        .group_by_dynamic('Time', every='6h', group_by='Station')
        .agg(pl.col(['Rainfall']).sum())
        .sort('Time')
    ),
    x='Time',
    y='Rainfall',
    color='Station',
    x_label='Time',
    y_label='Rainfall (mm)',
)

station_name_list = metrics.unique(subset=['Station']).sort('Station').select('Station').collect().to_series().to_list()


def create_metric_section():
    st.subheader('3-Day Rainfall Average (mm/d)')
    a, b, c, d, e = st.columns(5)
    for col, station in zip(
        [a, b, c, d, e],
        station_name_list,
    ):
        val = (
            filter_metrics_time_period(station, number_days=3).item()
            if (len(filter_metrics_time_period(station, number_days=3)) > 0)
            else 0
        ) / 3
        delta = (val - filter_metrics_time_period(station, number_days=7).item()) / 7
        col.metric(
            label=station,
            value=(str(round(val, 1)) + ' ' + get_rainfall_emoji(val)),
            delta=round(delta, 1),
        )
    with st.expander('Further Information'):
        st.text('Delta values indicate difference between 3-day average and 14-day average.')
        st.info('Data Sources: MeteoSwiss')


def filter_metrics_time_period(station, number_days):
    return (
        metrics.filter((pl.col('Station') == station) & (pl.col('Time Period') == number_days))
        .select(pl.col('Rainfall'))
        .collect()
    )


create_metric_section()
