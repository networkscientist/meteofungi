import streamlit as st
import pandas as pd

# --- Load data ---
st.set_page_config(layout='wide')


@st.cache_data
def load_rainfall():
    return pd.read_parquet('rainfall.parquet')
# def load_rainfall_from_local(stations_rainfall, metadata):
#     return load_rainfall(stations_rainfall, metadata['precipitation'], from_local=True)

# meta = load_metadata()
rainfall = load_rainfall()
st.title('MeteoFungi')
st.area_chart(data=rainfall, x=None, y='Rainfall', color='Station', x_label='Time', y_label='Rainfall (mm)')
st.info('Source: MeteoSwiss')
