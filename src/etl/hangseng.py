from datetime import datetime
import collections

from bs4 import BeautifulSoup
import pandas as pd

from gizmo import rest_gizmo
from gizmo import math_gizmo

def _cleanHtml(val):
    tokens = [u'\xa0', '\t', '\n']
    for token in tokens:
        val = val.replace(token,'')
    return val

# rawHtml from Google https://finance.yahoo.com/quote/%5Ehsi/history?ltr=1
def parse_hsi_last_closing(rawHtml):
    historical_closing =  pd.DataFrame([x.split(',') for x in rawHtml.split('\n')])
    headers = historical_closing.iloc[0]
    historical_closing.columns = headers
    historical_closing.drop([0])
    return historical_closing

def get_hsi_tm1_closing(historical_closing):
    tm1_closing = 0
    dt = historical_closing.iloc[-1]['Date']
    dt = datetime.strptime(dt, '%Y-%m-%d')
    tm1_closing = float(historical_closing.iloc[-1]['Close'])

    return {
        "dt" : dt,
        "close" : tm1_closing
    }

# rawHtml from Hangseng.com https://eba.hangseng.com/eBAWeb/MINISITE_ENG?app=WarrantPut&Type=I
def parse_hk_warrant(rawHtml):
    instruments = []

    soup = BeautifulSoup(rawHtml, 'html.parser')
    tables = soup.find_all('table')
    for table in tables:
        rows = table.find_all('tr')
        if len(rows)>30: # Primary table contains many rows
            matches = [header for header in rows[0].find_all('td') if ('Strike' in header.text)] # and Primary table has column "Strike"
            if len(matches)>1:
                target_table = matches[0].find_all('table')[0]
                fields = ['code', 'name', 'underlying', 'call_put', 'K', 'S', 'price', 'turnover', 'maturity', 'os_qty', 'impl_vol', 'premium', 'gearing', 'conversion_ratio']

                i = 0
                for row in target_table.find_all('tr'):
                    if i>0:
                        instrument = {}
                        j = 0
                        for td in row.find_all('td'):
                            field_name = fields[j]
                            field_value = _cleanHtml(td.text)

                            if field_name in ['code', 'name', 'underlying', 'call_put']:
                                # string fields
                                field_value = field_value.strip().lower() if field_value else None
                            elif field_name in ['maturity']:
                                # date fields
                                field_value = datetime.strptime(field_value, '%Y/%m/%d')
                            else:
                                field_value = math_gizmo.convert_float(field_value)
                            instrument[field_name] = field_value
                            j+=1
                        instruments.append(instrument)
                    i+=1
                break
    instruments = pd.DataFrame(instruments)
    # Calculate maturity buckets
    today = datetime.today()
    instruments['months2maturity'] = instruments['maturity'].apply(lambda maturity : (maturity.year - today.year) * 12 + (maturity.month - today.month))    
    # Move analytics to right hand side
    instruments = instruments.reindex(columns=['code', 'name', 'underlying', 'maturity', 'months2maturity', 'conversion_ratio', 'K', 'S', 'price', 'turnover', 'os_qty', 'impl_vol', 'premium', 'gearing'])
    instruments.drop(['S'], axis=1)
    # instruments.set_index('code', inplace=True)
    return instruments

scenarioColumns = collections.namedtuple('scenario_columns',['spot','s_minus_k','payoff', 'payoff_minus_price', 'cheapest_today', 'cheapest_1m', 'cheapest_3m', 'cheapest_6m', 'cheapest_9m', 'cheapest_12m']) 

def _get_column_name(scenario):
    colname_spot = f'S @scenario={str(scenario)}'
    colname_k_minus_s = f'K-S @scenario={str(scenario)}'
    colname_payoff = f'payoff_per_warrant scenario={str(scenario)}'
    colname_payoff_minus_price = f'payoff_minus_price_per_warrant scenario={str(scenario)}'
    colname_cheapest_today = f'cheapest_today scenario={str(scenario)}'
    colname_cheapest_1m = f'cheapest_1m scenario={str(scenario)}'
    colname_cheapest_3m = f'cheapest_3m scenario={str(scenario)}'
    colname_cheapest_6m = f'cheapest_6m scenario={str(scenario)}'
    colname_cheapest_9m = f'cheapest_9m scenario={str(scenario)}'
    colname_cheapest_12m = f'cheapest_12m scenario={str(scenario)}'
    return scenarioColumns(colname_spot, colname_k_minus_s, colname_payoff, colname_payoff_minus_price, colname_cheapest_today, colname_cheapest_1m, colname_cheapest_3m, colname_cheapest_6m, colname_cheapest_9m, colname_cheapest_12m)

def calculate_payoff_per_warrant(s, scenario, instruments):
    column_names = _get_column_name(scenario)
    colname_spot = column_names.spot
    colname_k_minus_s = column_names.s_minus_k
    colname_payoff = column_names.payoff
    colname_payoff_minus_price = column_names.payoff_minus_price
    colname_cheapest_today = column_names.cheapest_today
    colname_cheapest_1m = column_names.cheapest_1m
    colname_cheapest_3m = column_names.cheapest_3m
    colname_cheapest_6m = column_names.cheapest_6m
    colname_cheapest_9m = column_names.cheapest_9m
    colname_cheapest_12m = column_names.cheapest_12m
    
    s = s * scenario
    instruments[colname_spot] = s
    instruments[colname_k_minus_s] = instruments['K'] - instruments[colname_spot]
    instruments[colname_payoff] = instruments[colname_k_minus_s]/instruments['conversion_ratio']
    instruments[colname_payoff_minus_price] = instruments[colname_payoff] - instruments['price']
    instruments.sort_values([colname_payoff_minus_price], ascending=[0], inplace=True)

    instruments[colname_cheapest_today] = False
    cheapest = instruments[instruments.months2maturity==0][colname_payoff_minus_price].max()
    instruments.loc[(instruments.months2maturity==0) & (instruments[colname_payoff_minus_price]==cheapest) & (instruments[colname_payoff_minus_price]>0), [colname_cheapest_today]] = True
    
    instruments[colname_cheapest_1m] = False
    cheapest = instruments[instruments.months2maturity==1][colname_payoff_minus_price].max()
    instruments.loc[(instruments.months2maturity==1) & (instruments[colname_payoff_minus_price]==cheapest) & (instruments[colname_payoff_minus_price]>0), [colname_cheapest_1m]] = True
    
    instruments[colname_cheapest_3m] = False
    cheapest = instruments[instruments.months2maturity==3][colname_payoff_minus_price].max()
    instruments.loc[(instruments.months2maturity==3) & (instruments[colname_payoff_minus_price]==cheapest) & (instruments[colname_payoff_minus_price]>0), [colname_cheapest_3m]] = True
    
    instruments[colname_cheapest_6m] = False
    cheapest = instruments[instruments.months2maturity==6][colname_payoff_minus_price].max()
    instruments.loc[(instruments.months2maturity==6) & (instruments[colname_payoff_minus_price]==cheapest) & (instruments[colname_payoff_minus_price]>0), [colname_cheapest_6m]] = True
    
    instruments[colname_cheapest_9m] = False
    cheapest = instruments[instruments.months2maturity==9][colname_payoff_minus_price].max()
    instruments.loc[(instruments.months2maturity==9) & (instruments[colname_payoff_minus_price]==cheapest) & (instruments[colname_payoff_minus_price]>0), [colname_cheapest_9m]] = True
    
    instruments[colname_cheapest_12m] = False
    cheapest = instruments[instruments.months2maturity==12][colname_payoff_minus_price].max()
    instruments.loc[(instruments.months2maturity==12) & (instruments[colname_payoff_minus_price]==cheapest) & (instruments[colname_payoff_minus_price]>0), [colname_cheapest_12m]] = True


if __name__ == '__main__':
    hk_warrants_hsi_put = 'https://eba.hangseng.com/eBAWeb/MINISITE_ENG?app=WarrantPut&Type=I'
    res = rest_gizmo.get(url=hk_warrants_hsi_put, dataType=None, as_df=False)
    instruments = parse_hk_warrant(res.content)
    hsi_s = 29000
    scenario_list = [0.9, 0.8, 0.7, 0.5]
    results = {}
    for scenario in scenario_list:
        calculate_payoff_per_warrant(hsi_s, scenario, instruments)
        results[scenario] = instruments