import streamlit as st
import pandas as pd

# --- Load data ---
st.set_page_config(layout='wide')
stations_jura = ['Aarberg', 'Bellelay', 'Couvet', 'Gadmen', 'Nesselboden']


@st.cache_data
def load_rainfall():
    return pd.read_parquet('rainfall.parquet')

rainfall = load_rainfall()
st.area_chart(data=rainfall, x=None, y=stations_jura, x_label='Time', y_label='Rainfall (mm)')
st.info('Source: MeteoSwiss')