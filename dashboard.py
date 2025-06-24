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


# def load_rainfall_from_local(stations_rainfall, metadata):
#     return load_rainfall(stations_rainfall, metadata['precipitation'], from_local=True)

# meta = load_metadata()
rainfall = load_rainfall()
metrics = load_metrics()
st.title('MeteoFungi')
st.area_chart(data=rainfall, x=None, y='Rainfall', color='Station', x_label='Time', y_label='Rainfall (mm)')

st.subheader('3-Day Rainfall Sum (mm)')
a, b, c, d, e = st.columns(5)
for col, station in zip([a, b, c, d, e], rainfall.Station.unique()):
    val = round(metrics[metrics.index == station].values.tolist()[0][0], 2)
    if val < 5:
        emo = '☀️'
    elif (val >= 5) & (val < 10):
        emo = '🌦️'
    elif val >= 10:
        emo = '🌧️'
    else:
        emo = '🌊'
    col.metric(label=station, value=str(val) + emo)
st.info('Source: MeteoSwiss')
