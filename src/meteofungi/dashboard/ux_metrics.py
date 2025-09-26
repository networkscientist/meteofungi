"""Provide static data for the MeteoShrooms dashboard ui"""

import streamlit as st


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
    if 0 < val < 1:
        return '☀️'  # No rain
    if 1 <= val < 10:
        return '🌦️'  # Light rain
    if 10 <= val < 20:
        return '🌧️'  # Moderate rain
    if 20 <= val < 50:
        return '🌊'  # Heavy rain
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
