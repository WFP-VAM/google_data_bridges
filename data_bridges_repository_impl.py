import os
import time
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from enum import Enum
from typing import Dict, Optional

import data_bridges_client
import pandas as pd
from data_bridges_client import ApiException
from data_bridges_client.token import WfpApiToken

from logger import logger


class EndpointType(Enum):
    CURRENCY_USD_QUOTE = ("vamdatabridges_currency-usdindirectquotation_get",
                          ['country_iso3', 'currency_name', 'page', 'env', 'format'],
                          "https://github.com/WFP-VAM/DataBridgesAPI")
    ECONOMIC_DATA_LIST = ("vamdatabridges_economicdata-indicatorlist_get",
                          ['page', 'indicator_name', 'iso3', 'env', 'format'],
                          "https://github.com/WFP-VAM/DataBridgesAPI")
    ECONOMIC_DATA_VALUES = ("vamdatabridges_economicdata_get",
                            ['indicator_name', 'page', 'iso3', 'start_date', 'end_date', 'env', 'format'],
                            "https://worldfoodprogramme.visualstudio.com/Digital%20Core/_workitems/edit/257384/")

    def __new__(cls, scope, params, description):
        obj = object.__new__(cls)
        obj._value_ = scope
        obj.params = params
        obj.description = description
        return obj

    @classmethod
    def from_label(cls, label):
        for member in cls:
            if member.value == label:
                return member
        raise ValueError(f"No type with label '{label}' found")


class DataBridgesRepository:
    def _refresh_access_token(self, key: str, secret: str, scopes: Optional[str]):
        pass

    def fetch_data_one_page(self, endpoint: EndpointType, **params: Dict) -> object:
        pass

    def get_total_pages(self, endpoint_type: EndpointType, params):
        pass

    def fetch_all_data_bridges_data(self, endpoint_type: EndpointType, **params) -> pd.DataFrame:
        pass

    def fetch_one_data_bridges_page(self, endpoint_type: EndpointType, params) -> pd.DataFrame:
        pass

    @staticmethod
    def normalize_items(response_items):
        pass


class DataBridgesRepositoryImpl(DataBridgesRepository):
    TOKEN_URL = "https://api.wfp.org/token"
    HOST = "https://api.wfp.org/vam-data-bridges/4.1.0"
    DATA_BRIDGES_THREADS = 5
    CURRENT_MAX_DATA_BRIDGES_PAGE_SIZE = 1000

    def __init__(self, key: str, secret: str, scopes: []):
        self.key = key
        self.secret = secret
        self.scopes = scopes
        self.configuration = data_bridges_client.Configuration(host=self.HOST)
        self._refresh_access_token(self.key, self.secret, self.scopes)
        self.api_client = data_bridges_client.ApiClient(self.configuration)
        self._set_api_instances(self.api_client)
        self.endpoint_dict = {
            # Methods required
            EndpointType.CURRENCY_USD_QUOTE: self.api_instance_currency.currency_usd_indirect_quotation_get,
            EndpointType.ECONOMIC_DATA_LIST: self.api_instance_economic.economic_data_indicator_list_get,
            EndpointType.ECONOMIC_DATA_VALUES: self.api_instance_economic.economic_data_indicator_name_get
        }

    def _refresh_access_token(self, key: str, secret: str, scopes: Optional[str]):
        logger.info("Token refresh called!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        max_retries = 5
        backoff_factor = 2
        for attempt in range(max_retries):
            try:
                token = WfpApiToken(api_key=key, api_secret=secret)
                self.configuration.access_token = token.refresh(scopes=scopes)
                logger.info("Access token refreshed successfully.")
                return
            except ApiException as e:
                if e.status in [429, 500, 502, 503, 504]:
                    wait_time = backoff_factor ** attempt
                    logger.warning(f"Token refresh failed with status {e.status}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to refresh access token: {e}")
                    raise
            except Exception as e:
                logger.error(f"Unexpected error during token refresh: {e}")
                raise

        raise Exception("Max retries reached while trying to refresh the access token.")

    def fetch_data_one_page(self, endpoint: EndpointType, **params: Dict) -> object:
        endpoint_method = self.endpoint_dict.get(endpoint)
        if not endpoint_method:
            logger.error(f"Invalid endpoint: {endpoint}")
            return None
        try:
            response = endpoint_method(**params)
            return response
        except ApiException as e:
            if e.status == 401:
                logger.info("Token expired, refreshing token.")
                self._refresh_access_token(self.key, self.secret, self.scopes)
                self.api_client = data_bridges_client.ApiClient(self.configuration)
                self._set_api_instances(self.api_client)
                endpoint_method = self.endpoint_dict[endpoint]
                return endpoint_method(**params)
            if e.status in [429, 500, 502, 503, 504]:
                logger.warning(f"API Exception: {e}. Retrying with exponential backoff.")
                return self._retry_with_backoff(endpoint_method, **params)

            logger.error(f"API Exception: {e}")
            raise
        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            raise

    @staticmethod
    def _retry_with_backoff(endpoint_method, **params):
        max_retries = 10
        backoff_factor = 2
        for attempt in range(max_retries):
            try:
                return endpoint_method(**params)
            except ApiException as e:
                if e.status in [429, 500, 502, 503, 504]:
                    wait_time = backoff_factor ** attempt
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    raise
        raise Exception("Max retries reached")

    def _set_api_instances(self, api_client):
        self.api_instance_market = data_bridges_client.MarketsApi(api_client)
        self.api_instance_market_prices = data_bridges_client.MarketPricesApi(api_client)
        self.api_instance_currency = data_bridges_client.CurrencyApi(api_client)
        self.api_instance_commodities = data_bridges_client.CommoditiesApi(api_client)
        self.api_instance_economic = data_bridges_client.EconomicDataApi(api_client)

    def fetch_all_data_bridges_data(self, endpoint_type: EndpointType, **params) -> pd.DataFrame:
        all_data_df = pd.DataFrame()
        _total_pages = self.get_total_pages(endpoint_type, params)

        futures = []
        with ThreadPoolExecutor(max_workers=self.DATA_BRIDGES_THREADS) as executor:
            if 'page' not in [params]:
                params['page'] = 1
            for page in range(params['page'], _total_pages + 1):
                page_params = params.copy()
                page_params['page'] = page
                futures.append(executor.submit(self.fetch_one_data_bridges_page, endpoint_type, page_params))

            for future in as_completed(futures):
                res_df = future.result()
                all_data_df = pd.concat([all_data_df, res_df], ignore_index=True)

        return all_data_df

    def get_total_pages(self, endpoint_type: EndpointType, params: dict) -> int:
        res = self.fetch_data_one_page(endpoint_type, **params)
        if hasattr(res, 'total_items'):  # TODO
            total_items = res.total_items
            page_size = params.get('page_size', self.CURRENT_MAX_DATA_BRIDGES_PAGE_SIZE)
            _total_pages = (total_items + page_size - 1) // page_size
            logger.warning(f"\nParams: {params} - Items per Page: {page_size} items\n"
                           f"Total Pages: {_total_pages} - (Total Items: ]|{total_items}|[)")
            return _total_pages
        return 1

    def fetch_one_data_bridges_page(self, endpoint_type: EndpointType, params: dict) -> pd.DataFrame:
        res = self.fetch_data_one_page(endpoint_type, **params)
        logger.info(f"Fetching {endpoint_type.value} data - bridges page with params: {params}")
        if hasattr(res, 'items'):
            normalized_items = self.normalize_items(res.items)
            return pd.DataFrame(normalized_items)
        logger.warning("No response data")
        return pd.DataFrame()

    @staticmethod
    def normalize_items(response_items):
        if isinstance(response_items, list) and response_items and hasattr(response_items[0], '__dict__'):
            return [obj.__dict__ for obj in response_items]
        return response_items


# EXAMPLE USAGE
if __name__ == "__main__":
    import dotenv

    # load in env key and secret
    dotenv.load_dotenv(".env")
    _key = os.getenv('DATA_BRIDGES_KEY')
    _secret = os.getenv("DATA_BRIDGES_SECRET")

    # get allowed the allowed scopes/endpoints
    datab_scopes = [scope.value for scope in EndpointType]
    # create a class instance
    d_bridges = DataBridgesRepositoryImpl(_key, _secret, datab_scopes)

    # required scope/method arguments (list of parameters can be obtained are at https://github.com/WFP-VAM/DataBridgesAPI)
    # economic args: https://github.com/WFP-VAM/DataBridgesAPI/blob/main/docs/EconomicDataApi.md
    # currency args: https://github.com/WFP-VAM/DataBridgesAPI/blob/main/docs/CurrencyApi.md
    currency_params = {
        'country_iso3': 'NGA',
        'env': 'prod',
        'page': 3
    }

    # fetches 1 page of raw data (1000 records)
    response_raw_data_page_3 = d_bridges.fetch_data_one_page(EndpointType.CURRENCY_USD_QUOTE, **currency_params)
    logger.info(f"API raw response for page 3: {response_raw_data_page_3}")

    # loops automatically through pages & fetches all data to a DataFrame
    response_df = d_bridges.fetch_all_data_bridges_data(EndpointType.CURRENCY_USD_QUOTE, **currency_params)
    logger.info(f"API dataframe response: {response_df}")
