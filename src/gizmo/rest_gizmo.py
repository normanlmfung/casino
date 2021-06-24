import io
import enum
import logging
import requests
import pandas as pd

class DataType(enum.Enum):
    csv = 1
    json = 2

def call_web_api(method, url, dataType, as_df=True, headers=None, data=None, json=None, **kwargs):
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO, handlers=[logging.StreamHandler()])
    logging.info(url)

    rsp = requests.request(method, url, headers=headers, data=data, json=json, **kwargs)
    if rsp.status_code != 200:
        raise Exception(f'Oops! error code: {rsp.status_code}, response: {rsp.content}')

    if as_df:
        if dataType == DataType.csv:
            csv = io.TextIOWrapper(io.BytesIO(rsp.content), encoding='utf-8')
            df = pd.read_csv(csv)
        elif dataType == DataType.json:
            df = pd.DataFrame(rsp.json())
        return df
    else:
        return rsp

def get(url, dataType, as_df=True, headers=None, data=None, json=None, **kwargs):
    return call_web_api("get", url, dataType, as_df, headers, data, json, **kwargs)

def post(url, dataType, as_df=True, headers=None, data=None, json=None, **kwargs):
    return call_web_api("post", url, dataType, as_df, headers, data, json, **kwargs)

