import requests


class OpenMetoClient:
    def __init__(self):
        self.api_url = 'https://api.open-meteo.com/v1/forecast'

    def get_parameter_by_location(self,
                                  longitude,
                                  latitude,
                                  parameters,
                                  time='hourly',
                                  include_current_weather=False,
                                  start_date=None,
                                  end_date=None):

        longitude = round(longitude, 2)
        latitude = round(latitude, 2)

        payload = {'latitude': str(latitude),
                   'longitude': str(longitude),
                   time: parameters
                   }

        if include_current_weather:
            payload['current_weather'] = 'true'

        if start_date is not None and end_date is not None:
            payload['start_date'] = start_date
            payload['end_date'] = end_date

        get_response = requests.get(self.api_url, params=payload)

        return get_response.json()

    def get_parameter_for_multiple_locations(self,
                                             longitudes,
                                             latitudes,
                                             parameters,
                                             time='hourly',
                                             include_current_weather=False,
                                             start_date=None,
                                             end_date=None):

        data = []
        for lon, lat in zip(longitudes, latitudes):
            response = self.get_parameter_by_location(lon,
                                                      lat,
                                                      parameters,
                                                      time,
                                                      include_current_weather,
                                                      start_date,
                                                      end_date)
            data.append(response)

        return data

    def get_default_forecast_for_location(self,
                                          longitude,
                                          latitude):

        default_forecast_parameters = ['temperature_2m',
                                       'apparent_temperature',
                                       'pressure_msl',
                                       'cloudcover',
                                       'windspeed_10m',
                                       'winddirection_10m']

        return self.get_parameter_by_location(longitude,
                                              latitude,
                                              default_forecast_parameters)

    def get_default_forecast_for_multiple_locations(self,
                                                    longitude,
                                                    latitude):

        data = []
        for lon, lat in zip(longitude, latitude):
            response = self.get_default_forecast_for_location(lon,
                                                              lat)

            data.append(response)

        return data