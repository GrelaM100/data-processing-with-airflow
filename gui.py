import streamlit as st
import datetime
import pandas as pd
import pydeck as pdk
import altair as alt


def divide_higher_lower_data(data, parameter):
    higher_data = data[data[parameter] >= data[parameter].median()]
    lower_data = data[data[parameter] < data[parameter].median()]
    higher_data['min_val'] = higher_data[parameter].min()
    higher_data['max_val'] = higher_data[parameter].max()
    lower_data['min_val'] = lower_data[parameter].min()
    lower_data['max_val'] = lower_data[parameter].max()

    return higher_data, lower_data


def prepare_visualization_for_parameter(parameter):
    forecasts['parameter'] = forecasts[parameter]
    plot_data = forecasts[forecasts['datetime'] == slider]
    higher, lower = divide_higher_lower_data(plot_data, parameter)

    if parameter == 'temperature' or parameter == 'apparent_temperature':
        unit = '\N{DEGREE SIGN}C'
    elif parameter == 'pressure':
        unit = 'hPa'
    elif parameter == 'cloudcover':
        unit = '%'
    else:
        unit = 'km/h'

    unit += '<br/>'

    tooltip = {
        "html":
            "<b>City:</b> {city} <br/>"
            f"<b>{parameter.capitalize()}:</b>"
            "{parameter}"
            f"{unit}",
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
                data=higher,
                pickable=True,
                opacity=0.8,
                radius_scale=6,
                radius_min_pixels=5,
                radius_max_pixels=100,
                line_width_min_pixels=1,
                onClick=True,
                filled=True,
                get_position=['longitude', 'latitude'],
                get_color=['((parameter - min_val) * 255) / (max_val - min_val)', '0', '0'],
                get_line_color=[0, 0, 0]),
            pdk.Layer(
                type='ScatterplotLayer',
                data=lower,
                pickable=True,
                opacity=0.8,
                radius_scale=6,
                radius_min_pixels=5,
                radius_max_pixels=100,
                line_width_min_pixels=1,
                onClick=True,
                filled=True,
                get_position=['longitude', 'latitude'],
                get_color=['0', '0', '255 - (((parameter - min_val) * 255) / (max_val - min_val))'],
                get_line_color=[0, 0, 0]),
        ],
        tooltip=tooltip)
    )

    st.dataframe(plot_data[['city', parameter]].reset_index(drop=True), width=400)
    st.markdown('''---''')
    option = st.selectbox('Select a city', plot_data['city'].sort_values(), index=35)

    data_selected_city = forecasts[forecasts['city'] == option]

    alt_chart = alt.Chart(data_selected_city).mark_line(
        point=alt.OverlayMarkDef(color='#F63366'),
        color='#262730'
    ).encode(
        x=alt.X('datetime',
                axis=alt.Axis(title='Time')),
        y=alt.Y('parameter',
                axis=alt.Axis(title=parameter.capitalize().replace('_', '  ')),
                scale=alt.Scale(domain=[data_selected_city[parameter].min(), data_selected_city[parameter].max()])),
    ).interactive()
    st.altair_chart(alt_chart, use_container_width=True)


with st.sidebar:
    option = st.radio(
        "Choose parameter",
        ('Temperature',
         'Apparent temperature',
         'Pressure',
         'Cloudcover',
         'Windspeed'))

    if option == 'Temperature':
        parameter = 'temperature'
    elif option == 'Apparent temperature':
        parameter = 'apparent_temperature'
    elif option == 'Pressure':
        parameter = 'pressure'
    elif option == 'Cloudcover':
        parameter = 'cloudcover'
    elif option == 'Windspeed':
        parameter = 'windspeed'

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

prepare_visualization_for_parameter(parameter)
