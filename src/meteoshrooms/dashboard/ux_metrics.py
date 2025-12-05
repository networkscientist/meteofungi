"""Provide static data for the MeteoShrooms dashboard ui"""

from typing import Sequence

import polars as pl
import streamlit as st
from polars import LazyFrame
from streamlit.delta_generator import DeltaGenerator

from meteoshrooms.dashboard.constants import (
    NUM_DAYS_DELTA,
    NUM_DAYS_VAL,
    PARAMETER_AGGREGATION_TYPES,
    WEATHER_SHORT_LABEL_DICT,
)
from meteoshrooms.dashboard.dashboard_utils import (
    META_PARAMETERS,
    WEATHER_COLUMN_NAMES_DICT,
)


def get_metric_emoji(val: float) -> str:
    """Retun emoji for rainfall intensity

    Parameters
    ----------
    val: float
        Rainfall intensity

    Returns
    -------
        Emoji representing rainfall intensity
    """
    if val < 0:
        val_below_zero_value_error_string: str = 'Value cannot be negative'
        raise ValueError(val_below_zero_value_error_string)
    if val < 1:
        return '☀️'  # No rain
    if val < 10:
        return '🌦️'  # Light rain
    if val < 20:
        return '🌧️'  # Moderate rain
    if val < 50:
        return '🌊'  # Heavy rain
    return '🌧️🌊'  # Very heavy rain


def create_metrics_expander_info(num_days_value: float, num_days_delta: float):
    """Add a Streamlit expander element with info on time aggregation

    Parameters
    ----------
    num_days_value: float
        Number of days over which averaging has been done for the metric
    num_days_delta: float
        Number of days, whose average has been take as a comparison
    """
    with st.expander('Further Information'):
        st.text(
            f'Delta values indicate difference between {num_days_value}-day average and {num_days_delta}-day average.'
        )
        st.info('Data Sources: MeteoSwiss')


def create_metric_tooltip_string(metric_name: str) -> str:
    return f'{WEATHER_COLUMN_NAMES_DICT[metric_name]} in {META_PARAMETERS.filter(pl.col("parameter_shortname") == metric_name).select("parameter_unit").item()}'


def create_metric_kwargs(metric_name) -> dict[str, bool | str]:
    return {
        'border': True,
        'help': create_metric_tooltip_string(metric_name),
        'height': 'stretch',
    }


def filter_metrics_time_period(
    metrics: pl.LazyFrame, station_name: str, number_days: int, metric_short_code: str
) -> pl.LazyFrame | None:
    return (
        metrics.filter(
            (pl.col('station_name') == station_name)
            & (pl.col('time_period') == number_days)
        ).select(pl.col(metric_short_code))
        if metrics.select(pl.len()).collect().item() > 0
        else None
    )


def calculate_metric_value(
    metrics: pl.LazyFrame, metric_name: str, station_name: str, number_days: int
) -> float | None:
    try:
        df_filtered: LazyFrame | None = filter_metrics_time_period(
            metrics, station_name, number_days, metric_name
        )
        if df_filtered is not None:
            if metric_name in PARAMETER_AGGREGATION_TYPES['sum']:
                df_filtered = df_filtered.select(pl.col(metric_name) / number_days)
            return df_filtered.collect().item()
        return None
    except ValueError:
        # If a station has data missing, return None
        return None


def create_metric_section(
    metrics: pl.LazyFrame, station_name: str, metrics_list: Sequence[str]
):
    st.subheader(station_name)

    cols_metric: list[DeltaGenerator] = st.columns(len(metrics_list))
    for col, metric_name in zip(
        cols_metric,
        metrics_list,
        strict=False,
    ):
        val: float | None = calculate_metric_value(
            metrics, metric_name, station_name, number_days=NUM_DAYS_VAL
        )

        metric_label: str = WEATHER_SHORT_LABEL_DICT[metric_name]
        if val is not None:
            delta: str | None = calculate_metric_delta(
                metric_name, metrics, station_name, val
            )
            col.metric(
                label=metric_label,
                value=convert_metric_value_to_string_for_metric_section(
                    metric_name, val
                ),
                delta=delta,
                **create_metric_kwargs(metric_name),
            )
        else:
            col.metric(
                label=metric_label, value='-', **create_metric_kwargs(metric_name)
            )


def calculate_metric_delta(
    metric_name: str, metrics: pl.LazyFrame, station_name: str, val: float
) -> str:
    val_delta: float | None = calculate_metric_value(
        metrics, metric_name, station_name, number_days=NUM_DAYS_DELTA
    )
    if val_delta:
        return str(
            round(val - val_delta, 1),
        )
    return '-'


def convert_metric_value_to_string_for_metric_section(
    metric_name: str, val: float
) -> str:
    return ' '.join(
        (
            str(round(val, 1)),
            (get_metric_emoji(val) if metric_name == 'rre150h0' else ''),
        )
    )
