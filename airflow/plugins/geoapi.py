import requests
import pandas as pd


class GeoApiClient:
    dataset: str = "geonames-all-cities-with-a-population-1000"
    endpoint: str = f"https://public.opendatasoft.com/api/v2/catalog/datasets/{dataset}/records"

    def __init__(
        self,
        country: str,
        offset: int,
    ) -> None:
        self.country = country
        self.offset = offset
        self.params = {
            "refine": f"label_en:{self.country}",
            "limit": 100,
            "timezone": "UTC"
        }
        self.max_api_calls = 7

    def extract(self) -> pd.DataFrame:
        n_requests: int = 0
        df_geonames = pd.DataFrame([])

        while n_requests <= self.max_api_calls:
            self.params['offset'] = n_requests * 100 + self.offset
            response = requests.get(
                url=self.endpoint,
                params=self.params
            ).json()

            entries = []
            for city_record in response['records']:
                fields = city_record['record']['fields']
                name = fields['name']
                population = fields['population']
                longitude, latitude = list(fields['coordinates'].values())
                entries.append((name, population, longitude, latitude))

            df_geonames = pd.concat([
                df_geonames, pd.DataFrame(entries)
            ], axis=0)

            n_requests += 1

        df_geonames.columns = ['city', 'population', 'longitude', 'latitude']

        return df_geonames
