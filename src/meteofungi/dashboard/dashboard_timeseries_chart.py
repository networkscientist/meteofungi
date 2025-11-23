from datetime import datetime, timedelta
from typing import Sequence
from zoneinfo import ZoneInfo

import polars as pl
import streamlit as st

from meteofungi.constants import TIMEZONE_SWITZERLAND_STRING
from meteofungi.dashboard.constants import NUM_DAYS_DELTA
from meteofungi.dashboard.dashboard_utils import WEATHER_COLUMN_NAMES_DICT
from meteofungi.data_preparation.constants import EXPR_WEATHER_AGGREGATION_TYPES


def create_area_chart_frame(
    frame_weather: pl.LazyFrame, stations_options_selected: Sequence[str]
):
    return (
        frame_weather.sort('reference_timestamp')
        .filter(
            (
                pl.col('reference_timestamp')
                >= (
                    datetime.now(tz=ZoneInfo(TIMEZONE_SWITZERLAND_STRING))
                    - timedelta(days=NUM_DAYS_DELTA)
                )
            )
            & (pl.col('station_name').is_in(stations_options_selected))
        )
        .group_by_dynamic('reference_timestamp', every='6h', group_by='station_name')
        .agg(EXPR_WEATHER_AGGREGATION_TYPES)
        .with_columns(pl.selectors.numeric().round(1))
        .rename(WEATHER_COLUMN_NAMES_DICT)
    )


def create_area_chart(
    df_weather: pl.LazyFrame, stations_options_selected: Sequence[str]
):
    st.area_chart(
        data=create_area_chart_frame(df_weather, stations_options_selected),
        x='Time',
        y='Precipitation',
        color='Station',
        x_label='Time',
        y_label='Rainfall (mm)',
    )
