import streamlit as st
import pandas as pd
from data_preparation import load_rainfall, stations_rainfall, load_metadata

# --- Load data ---
st.set_page_config(layout='wide')


@st.cache_data
def load_rainfall_from_local(stations_rainfall, metadata):
    return load_rainfall(stations_rainfall, metadata['precipitation'], from_local=True)


# def load_rainfall():
#     return pd.read_parquet('rainfall.parquet')

meta = load_metadata()
rainfall = load_rainfall_from_local(stations_rainfall, meta)
st.title('MeteoFungi')
st.area_chart(data=rainfall, x=None, y='Rainfall', color='Station', x_label='Time', y_label='Rainfall (mm)')
st.info('Source: MeteoSwiss')
