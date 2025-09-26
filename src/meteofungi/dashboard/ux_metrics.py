"""Provide static data for the MeteoShrooms dashboard ui"""

import streamlit as st


def get_metric_emoji(val):
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


def create_metrics_expander_info(num_days_value, num_days_delta):
    with st.expander('Further Information'):
        st.text(
            f'Delta values indicate difference between {num_days_value}-day average and {num_days_delta}-day average.'
        )
        st.info('Data Sources: MeteoSwiss')
