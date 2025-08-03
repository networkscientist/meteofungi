import streamlit as st
from datetime import datetime, timedelta

import polars as pl

from ux_metrics import get_rainfall_emoji, create_metrics_expander_info

# --- Load data ---
st.set_page_config(layout='wide', initial_sidebar_state='expanded')

TIME_PERIODS = {period: (datetime.now() - timedelta(days=period)) for period in [3, 7, 14, 30]}
METRICS_LIST = ['rre150h0', 'tre200h0', 'ure200h0', 'fu3010h0', 'tde200h0']
PARAMETER_AGGREGATION_TYPES = {'sum': ['rre150h0'], 'mean': ['tre200h0', 'ure200h0', 'fu3010h0', 'tde200h0']}


@st.cache_data
def load_weather_data():
    return pl.scan_parquet('weather_data.parquet')


@st.cache_data
def create_metrics(_df, time_periods):
    return pl.concat(
        [
            _df.filter(pl.col('reference_timestamp') >= datetime_period)
            .drop('reference_timestamp')
            .group_by(['station_abbr', 'station_name'])
            .agg(pl.sum(*PARAMETER_AGGREGATION_TYPES['sum']), pl.mean(*PARAMETER_AGGREGATION_TYPES['mean']))
            .with_columns(pl.lit(period).alias('time_period'))
            for period, datetime_period in time_periods.items()
        ]
    )


@st.cache_data
def load_meta_stations():
    return pl.scan_parquet('meta_stations.parquet')


@st.cache_data
def load_meta_params():
    return pl.scan_parquet('ogd-smn_meta_parameters.parquet')


meta_stations = load_meta_stations()
meta_params = load_meta_params()
METRICS_NAMES_DICT = {
    metric: meta_params.filter(pl.col('parameter_shortname') == metric)
    .select(pl.col('parameter_description_en').str.to_titlecase().str.extract(r'([\w\s()]+)', 1))
    .collect()
    .item()
    for metric in METRICS_LIST
}
METRICS_CATEGORY_DICT = {
    metric: meta_params.filter(pl.col('parameter_shortname') == metric)
    .select(pl.col('parameter_group_en').str.to_titlecase())
    .collect()
    .item()
    for metric in METRICS_LIST
}
WEATHER_COLUMN_NAMES_DICT = dict({'reference_timestamp': 'Time', 'station_name': 'Station'} | METRICS_NAMES_DICT)
df_weather = load_weather_data()
metrics = create_metrics(df_weather, TIME_PERIODS)
station_name_list = (
    metrics.unique(subset=['station_name']).sort('station_name').select('station_name').collect().to_series().to_list()
)

st.title('MeteoFungi')

with st.sidebar:
    st.title('Stations')
    stations_options_selected = st.multiselect(
        'Stations:', station_name_list, default=station_name_list[0], max_selections=5, placeholder='Choose Station(s)'
    )


st.area_chart(
    data=(
        df_weather.sort('reference_timestamp')
        .filter(
            (pl.col('reference_timestamp') >= (datetime.now() - timedelta(days=7)))
            & (pl.col('station_name').is_in(stations_options_selected))
        )
        .group_by_dynamic('reference_timestamp', every='6h', group_by='station_name')
        .agg(pl.sum(*PARAMETER_AGGREGATION_TYPES['sum']), pl.mean(*PARAMETER_AGGREGATION_TYPES['mean']))
        # .sort('reference_timestamp')
        .with_columns(pl.selectors.numeric().round(1))
        .rename(WEATHER_COLUMN_NAMES_DICT)
    ),
    x='Time',
    y='Precipitation',
    color='Station',
    x_label='Time',
    y_label='Rainfall (mm)',
)


def create_metric_section(station_name, metrics_list):
    st.subheader(station_name)

    cols_metric = st.columns(len(metrics_list))
    for col, metric in zip(
        cols_metric,
        metrics_list,
    ):
        val = calculate_metric_value(metric, station_name, number_days=3)
        delta = calculate_metric_delta(metric, station_name, val, number_days=7)
        if val:
            col.metric(
                label=WEATHER_COLUMN_NAMES_DICT[metric],
                value=(
                    str(round(val, 1))
                    + ' '
                    + meta_params.filter(pl.col('parameter_shortname') == metric)
                    .select('parameter_unit')
                    .collect()
                    .item()
                    + ' '
                    + (get_rainfall_emoji(val) if metric == 'rre150h0' else '')
                ),
                delta=str(round(delta, 1)),
            )


def calculate_metric_delta(metric, station_name, value, number_days):
    if value:
        if metric in PARAMETER_AGGREGATION_TYPES['sum']:
            return (
                value
                - filter_metrics_time_period(station_name, number_days=number_days, metric_short_code=metric).item()
            ) / number_days
        elif metric in PARAMETER_AGGREGATION_TYPES['mean']:
            return (
                value
                - filter_metrics_time_period(station_name, number_days=number_days, metric_short_code=metric).item()
            )


def calculate_metric_value(metric, station_name, number_days):
    if metric in PARAMETER_AGGREGATION_TYPES['sum']:
        return (
            filter_metrics_time_period(station_name, number_days=number_days, metric_short_code=metric).item()
            if (len(filter_metrics_time_period(station_name, number_days=number_days, metric_short_code=metric)) > 0)
            else 0
        ) / number_days
    elif metric in PARAMETER_AGGREGATION_TYPES['mean']:
        return (
            filter_metrics_time_period(station_name, number_days=number_days, metric_short_code=metric).item()
            if (len(filter_metrics_time_period(station_name, number_days=number_days, metric_short_code=metric)) > 0)
            else 0
        )


def filter_metrics_time_period(station_name, number_days, metric_short_code):
    return (
        metrics.filter((pl.col('station_name') == station_name) & (pl.col('time_period') == number_days))
        .select(pl.col(metric_short_code))
        .collect()
    )


for station in stations_options_selected:
    create_metric_section(station, METRICS_LIST)
create_metrics_expander_info()
