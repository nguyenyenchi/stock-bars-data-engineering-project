import requests

# Naming an object of class that talks to a server
class AlpacaApiClient: # Talks to server

    def __init__(self, api_key_id, api_secret_key):
        self.base_url = "https://data.alpaca.markets/v2/stocks/bars"
        
        if api_key_id is None:
            raise Exception("API key cannot be set to None.")
        self.api_key_id = api_key_id

        if api_secret_key is None:
            raise Exception("API secret key cannot be set to None.")
        self.api_secret_key = api_secret_key

    def get_alpaca_api_data(self,
            symbols: str, 
            timeframe: str, 
            start_time: str,
            end_time: str
            ) -> list[dict]:
        
        base_url = f"{self.base_url}?limit=1000&adjustment=raw&feed=sip&sort=asc"
        params = {"symbols": symbols, "timeframe": timeframe,"start": start_time, "end": end_time}
        headers = {"APCA-API-KEY-ID": self.api_key_id, "APCA-API-SECRET-KEY": self.api_secret_key}
        response = requests.get(base_url, params=params, headers=headers)
        
        if response.status_code == 200 and response.json().get("bars") is not None:
            return response.json().get("bars")
        else: 
            raise Exception(
                f"Failed to extract data from Alpaca Market API. Status Code: {response.status_code}. Response: {response.text}"
        )

