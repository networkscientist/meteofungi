from datetime import datetime, timedelta
from typing import Sequence
from zoneinfo import ZoneInfo

import polars as pl
import streamlit as st

from meteoshrooms.constants import TIMEZONE_SWITZERLAND_STRING
from meteoshrooms.dashboard.constants import WEATHER_SHORT_LABEL_DICT
from meteoshrooms.dashboard.dashboard_utils import WEATHER_COLUMN_NAMES_DICT
from meteoshrooms.data_preparation.constants import EXPR_WEATHER_AGGREGATION_TYPES


def create_area_chart_frame(
    frame_weather: pl.LazyFrame,
    stations_options_selected: Sequence[str],
    time_period: int,
) -> pl.LazyFrame:
    return (
        frame_weather.sort('reference_timestamp')
        .filter(
            (
                pl.col('reference_timestamp')
                >= (
                    datetime.now(tz=ZoneInfo(TIMEZONE_SWITZERLAND_STRING))
                    - timedelta(days=time_period)
                )
            )
            & (pl.col('station_name').is_in(stations_options_selected))
        )
        .group_by_dynamic('reference_timestamp', every='6h', group_by='station_name')
        .agg(EXPR_WEATHER_AGGREGATION_TYPES)
        .with_columns(pl.selectors.numeric().round(1))
        .rename(WEATHER_COLUMN_NAMES_DICT)
    )


@st.cache_data
def create_area_chart(
    _df_weather: pl.LazyFrame,
    stations_options_selected: Sequence[str],
    time_period: int | None,
    param_short_code: str,
):
    if not time_period:
        time_period: int = 7
    st.area_chart(
        data=create_area_chart_frame(
            _df_weather, stations_options_selected, time_period
        ),
        x='Time',
        y='Precipitation',
        color='Station',
        x_label='Time',
        y_label=f'{WEATHER_SHORT_LABEL_DICT[param_short_code]} (mm)',
    )
