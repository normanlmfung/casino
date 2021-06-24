import logging
import os

def log_error(filename,errMsg):
    if filename != "" and os.path.isdir(os.path.dirname(os.path.abspath(filename))):
        logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO, handlers=[logging.FileHandler(filename),logging.StreamHandler()])
    else:
        logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO, handlers=[logging.StreamHandler()])
    logging.error(errMsg)

def log_info(filename,Msg):
    if filename != "" and os.path.isdir(os.path.dirname(os.path.abspath(filename))):
        logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO, handlers=[logging.FileHandler(filename),logging.StreamHandler()])
    else:
        logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO, handlers=[logging.StreamHandler()])
    logging.info(Msg)