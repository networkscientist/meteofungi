import streamlit as st
import pandas as pd

# --- Load data ---
st.set_page_config(layout='wide')


@st.cache_data
def load_rainfall():
    return pd.read_parquet('rainfall.parquet')


@st.cache_data
def load_metrics():
    return pd.read_parquet('metrics.parquet')


rainfall = load_rainfall()
metrics = load_metrics()
st.title('MeteoFungi')
st.area_chart(data=rainfall, x=None, y='Rainfall', color='Station', x_label='Time', y_label='Rainfall (mm)')

st.subheader('3-Day Rainfall Sum (mm)')
a, b, c, d, e = st.columns(5)
for col, station in zip([a, b, c, d, e], rainfall.Station.unique()):
    val = round(metrics[metrics.index == station].values.tolist()[0][0], 2)
    if val < 1:
        emo = '☀️'
    elif (val >= 1) & (val < 20):
        emo = '🌦️'
    elif (val >= 20) & (val < 50):
        emo = '🌧️'
    else:
        emo = '🌊'
    col.metric(label=station, value=str(val) + emo)
st.info('Source: MeteoSwiss')
