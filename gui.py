import streamlit as st
import datetime
import pandas as pd
import pydeck as pdk
import altair as alt

date_format = 'DD-MM-YYYY HH:mm'
forecasts = pd.read_parquet('forecasts.parquet')
forecasts['datetime'] = pd.to_datetime(forecasts['datetime'])
start_date = forecasts['datetime'].min().to_pydatetime()
end_date = forecasts['datetime'].max().to_pydatetime()

slider = st.slider('Select date',
                   min_value=start_date,
                   value=start_date,
                   max_value=end_date,
                   step=datetime.timedelta(hours=1),
                   format=date_format)

plot_data = forecasts[forecasts['datetime'] == slider]
plot_data['max_temp'] = plot_data['temperature'].max()
plot_data['min_temp'] = plot_data['temperature'].min()
tooltip = {
    "html":
        "<b>City:</b> {city} <br/>"
        "<b>Temperature:</b> {temperature}\N{DEGREE SIGN}C<br/>",
    "style": {
        "backgroundColor": "steelblue",
        "color": "black",
    }
}
st.pydeck_chart(pdk.Deck(
    map_provider='mapbox',
    map_style='mapbox://styles/mapbox/light-v9',
    initial_view_state=pdk.ViewState(
        latitude=51.91,
        longitude=19.14,
        zoom=5
    ),
    layers=[
        pdk.Layer(
            type='ScatterplotLayer',
            data=plot_data,
            pickable=True,
            opacity=0.8,
            radius_scale=6,
            radius_min_pixels=5,
            radius_max_pixels=100,
            line_width_min_pixels=1,
            onClick=True,
            filled=True,
            get_position=['longitude', 'latitude'],
            get_color=['((temperature - min_temp) * 255) / (max_temp - min_temp)', 0, 0],
            get_line_color=[0, 0, 0])
    ],
    tooltip=tooltip)
)

st.dataframe(plot_data[['city', 'temperature']].reset_index(drop=True), width=400)
st.markdown('''---''')
option = st.selectbox('Select a city', plot_data['city'].sort_values(), index=35)

data_selected_city = forecasts[forecasts['city'] == option]

alt_chart = alt.Chart(data_selected_city).mark_line(
    point=alt.OverlayMarkDef(color='#F63366'),
    color='#262730'
).encode(
    x=alt.X('datetime', axis=alt.Axis(title='Time')),
    y=alt.Y('temperature', axis=alt.Axis(title='Temperature')),
).interactive()
st.altair_chart(alt_chart, use_container_width=True)
