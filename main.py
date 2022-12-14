from open_meto_api import OpenMetoClient

if __name__ == '__main__':
    open_meto_client = OpenMetoClient()

    response = open_meto_client.get_default_forecast_for_multiple_locations([52.52, 50.07], [13.41, 19.91])
    print(response)
