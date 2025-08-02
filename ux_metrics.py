import streamlit as st


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


def create_metrics_expander_info():
    with st.expander('Further Information'):
        st.text('Delta values indicate difference between 3-day average and 14-day average.')
        st.info('Data Sources: MeteoSwiss')
