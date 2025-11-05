"""Provide static data for the MeteoShrooms dashboard ui"""

from typing import Sequence

import polars as pl
import streamlit as st
from streamlit.delta_generator import DeltaGenerator

from meteofungi.dashboard.constants import (
    NUM_DAYS_DELTA,
    NUM_DAYS_VAL,
    PARAMETER_AGGREGATION_TYPES,
    WEATHER_SHORT_LABEL_DICT,
)
from meteofungi.dashboard.dashboard_utils import (
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
    elif 0 < val < 1:
        return '☀️'  # No rain
    elif 1 <= val < 10:
        return '🌦️'  # Light rain
    elif 10 <= val < 20:
        return '🌧️'  # Moderate rain
    elif 20 <= val < 50:
        return '🌊'  # Heavy rain
    else:
        return '🌧️🌊'  # Very heavy rain


def create_metrics_expander_info(num_days_value: float, num_days_delta: float) -> None:
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
    return f'{WEATHER_COLUMN_NAMES_DICT[metric_name]} in {META_PARAMETERS.filter(pl.col("parameter_shortname") == metric_name).select("parameter_unit").collect().item()}'


def create_metric_kwargs(metric_name):
    metric_kwargs: dict[str, bool | str] = {
        'border': True,
        'help': create_metric_tooltip_string(metric_name),
        'height': 'stretch',
    }
    return metric_kwargs


def calculate_metric_value_if_greater_zero(
    metrics: pl.LazyFrame, metric_name: str, station_name: str, number_days: int
) -> int:
    return (
        filter_metrics_time_period(
            metrics,
            station_name,
            number_days=number_days,
            metric_short_code=metric_name,
        ).item()
        if (
            len(
                filter_metrics_time_period(
                    metrics,
                    station_name,
                    number_days=number_days,
                    metric_short_code=metric_name,
                )
            )
            > 0
        )
        else 0
    )


def filter_metrics_time_period(
    metrics: pl.LazyFrame, station_name: str, number_days: int, metric_short_code: str
) -> pl.DataFrame:
    return (
        metrics.filter(
            (pl.col('station_name') == station_name)
            & (pl.col('time_period') == number_days)
        )
        .select(pl.col(metric_short_code))
        .collect()
    )


def calculate_metric_value(
    metrics: pl.LazyFrame, metric_name: str, station_name: str, number_days: int
) -> float | None:
    if metric_name in PARAMETER_AGGREGATION_TYPES['sum']:
        return (
            calculate_metric_value_if_greater_zero(
                metrics, metric_name, station_name, number_days
            )
            / number_days
        )
    if metric_name in PARAMETER_AGGREGATION_TYPES['mean']:
        return calculate_metric_value_if_greater_zero(
            metrics, metric_name, station_name, number_days
        )
    return None


def calculate_metric_delta(
    metrics: pl.LazyFrame,
    metric_name: str,
    station_name: str,
    value: float | None,
    number_days: int,
) -> float | None:
    if value is not None:
        if metric_name in PARAMETER_AGGREGATION_TYPES['sum']:
            return (
                value
                - filter_metrics_time_period(
                    metrics,
                    station_name,
                    number_days=number_days,
                    metric_short_code=metric_name,
                ).item()
            ) / number_days
        if metric_name in PARAMETER_AGGREGATION_TYPES['mean']:
            return (
                value
                - filter_metrics_time_period(
                    metrics,
                    station_name,
                    number_days=number_days,
                    metric_short_code=metric_name,
                ).item()
            )
        return None
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
        delta: float | None = calculate_metric_delta(
            metrics, metric_name, station_name, val, number_days=NUM_DAYS_DELTA
        )
        metric_label: str = WEATHER_SHORT_LABEL_DICT[metric_name]
        if val is not None:
            col.metric(
                label=metric_label,
                value=(
                    str(round(val, 1))
                    + ' '
                    + (get_metric_emoji(val) if metric_name == 'rre150h0' else '')
                ),
                delta=str(round(delta, 1)),
                **create_metric_kwargs(metric_name),
            )
        else:
            col.metric(
                label=metric_label, value='-', **create_metric_kwargs(metric_name)
            )
