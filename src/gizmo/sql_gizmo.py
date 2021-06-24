import urllib
import pyodbc
import pandas as pd

from gizmo import config_gizmo as config

# SQLAlchemy engine
def get_engine(env, db_name, specific_settings_name = None):
    from sqlalchemy import create_engine
    from sqlalchemy.engine.url import URL
    app_config = config.load_settings(env, specific_settings_name)
    connstr = app_config.integration_config.databases[db_name]
    params = urllib.parse.quote_plus(connstr)
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    return engine
