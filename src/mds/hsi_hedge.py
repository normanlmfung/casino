import sys
import traceback
import argparse
from datetime import datetime
import json
from dotmap import DotMap

from gizmo import logging_gizmo
from gizmo import datetime_gizmo
from gizmo import config_gizmo
from gizmo import rest_gizmo
from gizmo import kafka_gizmo

from etl import hangseng

JOB_NAME = "hsi_warrants_mds"
logFilename=f"./shared/log/{JOB_NAME}_{datetime.now().strftime('%Y%m%d')}.log"

# Handler can be invoked directly from AWS Lambda
def hsi_hedge_handler(event, context): 
    context = DotMap(json.loads(context_json))
    env = context.env
    target_os = context.target_os
    dt = context.dt
    hsioverride = context.hsioverride
    target_url_hangseng_historical_closing = context.target_url_hangseng_historical_closing
    target_url_hangseng_warrants_put = context.target_url_hangseng_warrants_put
    kafka_brokers = context.kafka_brokers

    logging_gizmo.log_info(logFilename,f"Job parameters:")
    logging_gizmo.log_info(logFilename,f"env: {env}")
    logging_gizmo.log_info(logFilename,f"target_os: {target_os}")
    logging_gizmo.log_info(logFilename,f"Job date: {dt}")
    logging_gizmo.log_info(logFilename,f"hsioverride: {hsioverride}")
    logging_gizmo.log_info(logFilename,f"target_url_hangseng_historical_closing: {target_url_hangseng_historical_closing}")
    logging_gizmo.log_info(logFilename,f"target_url_hangseng_warrants_put: {target_url_hangseng_warrants_put}")

    # Step 1. Fetch HSI last close as hsi_s (But knowing it's not actually spot, it's tm1 closing)
    if hsioverride==0:
        res = rest_gizmo.get(url=target_url_hangseng_historical_closing, dataType=None, as_df=False)
        logging_gizmo.log_info(logFilename,f"Hang Seng spot prices, status: {str(res.status_code)}, content: {res.content}")
        historical_closing = hangseng.parse_hsi_last_closing(res.content.decode("utf-8"))
        tm1_closing = DotMap(hangseng.get_hsi_tm1_closing(historical_closing))
        tm1 = tm1_closing.dt
        hsi_s = tm1_closing.close
    else:
        hsi_s = hsioverride

    logging_gizmo.log_info(logFilename,f"Hang Seng closing on tm1: {str(tm1)}: {hsi_s}")

    # Step 2. Fetch HSI warrant prices https://eba.hangseng.com/eBAWeb/MINISITE_ENG?app=WarrantPut&Type=I
    res = rest_gizmo.get(url=target_url_hangseng_warrants_put, dataType=None, as_df=False)
    logging_gizmo.log_info(logFilename,f"Hang Seng warrant prices, status: {str(res.status_code)}, content: {res.content}")

    instruments = hangseng.parse_hk_warrant(res.content)
    scenario_list = [0.9, 0.8, 0.7, 0.5]
    for scenario in scenario_list:
        hangseng.calculate_payoff_per_warrant(hsi_s, scenario, instruments)
        logging_gizmo.log_info(logFilename,f"instruments, scenario: {str(scenario)} #rows: {str(instruments.shape[0])}")

    instruments_json = instruments.to_json()
    
    kafka_gizmo.publish(bootstrap_servers = kafka_brokers, topic = "hsi_hedge", messages = [instruments_json])

    return instruments_json

# main flow can be invoked from .bat on Windows and .sh on linux
try:
    if __name__ == "__main__":
        logging_gizmo.log_info(logFilename,f"{JOB_NAME} started")

        parser = argparse.ArgumentParser()
        parser.add_argument("--env", help="dev/uat/prod", default="dev")
        parser.add_argument("--os", help="win/linux", default="linux")
        parser.add_argument("--dt", help="yyyy-mm-dd, example 2021-06-09. If not specified default to today.", default=None)
        parser.add_argument("--hsioverride", help="Override HSI spot", default=0)
        args = parser.parse_args()
        env = args.env
        target_os = args.os
        dt = args.dt
        hsioverride = int(args.hsioverride)
        
        if not env:
            env = "dev"
        
        if not dt:
            dt = datetime_gizmo.today()
        else:
            dt = datetime.strptime(dt, '%Y-%m-%d')
        dt_str =  dt.strftime("%Y-%m-%d")

        config = config_gizmo.load_settings(env=env, specific_settings_name="hangseng")

        target_url_hangseng_historical_closing = config.queries.historical_closing
        target_url_hangseng_warrants_put = config.queries.warrants.put

        kafka_brokers = config.integration_config.message_buses.mds.kafka1
        kafka_brokers_urls = []
        for broker in kafka_brokers:
            kafka_brokers_urls.append(kafka_brokers[broker])

        event = None # Under AWS Lambda, passed by AWS.
        context = {
            "env" : env,
            "dt" : dt_str,
            "target_os" : target_os,
            "hsioverride" : hsioverride,
            "target_url_hangseng_historical_closing" : target_url_hangseng_historical_closing,
            "target_url_hangseng_warrants_put" : target_url_hangseng_warrants_put,
            "kafka_brokers" : kafka_brokers_urls
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
                        "--env", "dev",
                        "--os", "win",
                        "--hsioverride", "28000"
                    ]
        }
    ]
}
'''
