import sys
import traceback
import argparse
from datetime import datetime
import json
from dotmap import DotMap
import pandas as pd

from gizmo import logging_gizmo
from gizmo import datetime_gizmo
from gizmo import config_gizmo
from gizmo import kafka_gizmo
from gizmo import sql_gizmo # TO BE REMOVED

JOB_NAME = "hsi_warrants_order_generation"
logFilename=f"./shared/log/{JOB_NAME}_{datetime.now().strftime('%Y%m%d')}.log"

# Handler can be invoked directly from AWS Lambda
def hsi_hedge_handler(event, context): 
    context = DotMap(json.loads(context_json))
    env = context.env
    target_os = context.target_os
    dt = context.dt
    kafka_brokers = context.kafka_brokers
    book_size_hkd = context.book_size_hkd
    hedge_horizon_months = context.hedge_horizon_months
    hedge_scenario = context.hedge_scenario
    hedge_ratio = context.hedge_ratio

    logging_gizmo.log_info(logFilename,f"Job parameters:")
    logging_gizmo.log_info(logFilename,f"env: {env}")
    logging_gizmo.log_info(logFilename,f"target_os: {target_os}")
    logging_gizmo.log_info(logFilename,f"Job date: {dt}")
    logging_gizmo.log_info(logFilename,f"book_size_hkd: {book_size_hkd}")
    logging_gizmo.log_info(logFilename,f"hedge_horizon_months: {hedge_horizon_months}")
    logging_gizmo.log_info(logFilename,f"hedge_scenario: {hedge_scenario}")
    logging_gizmo.log_info(logFilename,f"hedge_ratio: {hedge_ratio}")

    sql_conn = None
    if target_os=="win":
        sql_conn = sql_gizmo.get_engine(env, 'casino_windows')
    elif target_os=="linux":
        sql_conn = sql_gizmo.get_engine(env, 'casino_linux')
    
    def _generate_order(message):
        instruments_json=str(message.value,'utf-8')
        instruments = pd.read_json(instruments_json)
        target_scenario_column_label = f"cheapest_{hedge_horizon_months}m scenario={hedge_scenario}"
        colname_payoff_minus_price_column_label = f'payoff_minus_price_per_warrant scenario={str(hedge_scenario)}'
        pnl_loss_long_size = book_size_hkd * (1-hedge_scenario) 
        hedge_size = pnl_loss_long_size * hedge_ratio
        scenario_columns = [column for column in instruments.columns if target_scenario_column_label==column]
        if len(scenario_columns)>0:
            target = instruments[(instruments.months2maturity==hedge_horizon_months) & (instruments[target_scenario_column_label]==True)]
            warrant_code = target.iloc[0]['code']
            warrant_name = target.iloc[0]['name']
            warrant_price = target.iloc[0]['price']
            warrant_turnover = target.iloc[0]['turnover']
            warrant_strike = target.iloc[0]['K']
            warrant_conversion_ratio = target.iloc[0]['conversion_ratio']
            payoff_minus_price_per_warrant = target.iloc[0][colname_payoff_minus_price_column_label]
            num_warrants_to_buy = int(hedge_size / payoff_minus_price_per_warrant)
            hedge_cost = num_warrants_to_buy * warrant_price

            order = {
                "warrant_code" : [warrant_code],
                "warrant_name" : [warrant_name],
                "warrant_strike" : [warrant_strike],
                "warrant_price" : [warrant_price],
                "warrant_turnover" : [warrant_turnover],
                "warrant_conversion_ratio" : [warrant_conversion_ratio],
                "payoff_minus_price_per_warrant" : [payoff_minus_price_per_warrant],
                "num_warrants_to_buy" : [num_warrants_to_buy],
                "hedge_cost" : [hedge_cost],
                "pnl_loss_long_size" : [pnl_loss_long_size], # expected loss given target hedge_scenario
                "hedge_size" : [hedge_size] # amount to hedge
            }

            logging_gizmo.log_info(logFilename,f"_generate_order: {message.topic}, order: {str(order)}")

            # Orders to be saved in database for easy tracking
            order = pd.DataFrame.from_dict(order)
            order.to_sql('hsi_hedge_orders', con=sql_conn, if_exists='append')

    kafka_gizmo.consume(
        bootstrap_servers = kafka_brokers,
        topic = "hsi_hedge",
        consumer_group_id= None,
        handlers = [_generate_order]
        )

    return {}   # Unreachable actually

# main flow can be invoked from .bat on Windows and .sh on linux
try:
    if __name__ == "__main__":
        logging_gizmo.log_info(logFilename,f"{JOB_NAME} started")

        parser = argparse.ArgumentParser()
        parser.add_argument("--env", help="dev/uat/prod", default="dev")
        parser.add_argument("--os", help="win/linux", default="linux")
        parser.add_argument("--dt", help="yyyy-mm-dd, example 2021-06-09. If not specified default to today.", default=None)
        args = parser.parse_args()
        env = args.env
        target_os = args.os
        dt = args.dt
        
        if not env:
            env = "dev"
        
        if not dt:
            dt = datetime_gizmo.today()
        else:
            dt = datetime.strptime(dt, '%Y-%m-%d')
        dt_str =  dt.strftime("%Y-%m-%d")

        config = config_gizmo.load_settings(env=env, specific_settings_name="hangseng")

        kafka_brokers = config.integration_config.message_buses.mds.kafka1
        kafka_brokers_urls = []
        for broker in kafka_brokers:
            kafka_brokers_urls.append(kafka_brokers[broker])

        book_size_hkd = config.strategy.book_size_hkd
        hedge_horizon_months = config.strategy.hedge_horizon_months
        hedge_scenario = config.strategy.hedge_scenario
        hedge_ratio = config.strategy.hedge_ratio

        event = None # Under AWS Lambda, passed by AWS.
        context = {
            "env" : env,
            "dt" : dt_str,
            "target_os" : target_os,
            "kafka_brokers" : kafka_brokers_urls,
            "book_size_hkd" : book_size_hkd,
            "hedge_horizon_months" : hedge_horizon_months,
            "hedge_scenario" : hedge_scenario,
            "hedge_ratio" : hedge_ratio
        } 
        context_json = json.dumps(context)
        result = hsi_hedge_handler(event, context_json)

except:
    errMsg = f"Error {JOB_NAME} {env} {str(sys.exc_info()[0])} {str(sys.exc_info()[1])} {traceback.format_exc()}"
    logging_gizmo.log_error(logFilename,errMsg)
    sys.exit(1)

finally:
    logging_gizmo.log_info(logFilename,f"{JOB_NAME} exiting")
    # cleanup if any

    sys.exit(0)

''' launch.json
{
    // Use IntelliSense to learn about possible attributes.
    // Hover to view descriptions of existing attributes.
    // For more information, visit: https://go.microsoft.com/fwlink/?linkid=830387
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python: Current File",
            "type": "python",
            "request": "launch",
            "program": "${file}",
            "console": "internalConsole",
            // Arguments to pass in by the debugger
            "args" : [
                    "--env", "prod",
                    "--dt", "2021-06-09",
                    "--os", "win"
                    ]
        }
    ]
}
'''
