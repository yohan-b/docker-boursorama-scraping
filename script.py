#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import logging
import time
import signal
import yaml
import requests
from threading import Event
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.DEBUG)

logging.info("====== Starting ======")

stop = Event()

def handler(signum, frame):
    global stop
    logging.info("Got interrupt: "+str(signum))
    stop.set()
    logging.info("Shutdown")

signal.signal(signal.SIGTERM,handler)
signal.signal(signal.SIGINT,handler)

with open('./conf.yml') as conf:
    yaml_conf = yaml.load(conf)
    stocks = yaml_conf.get("stocks")
    interval = yaml_conf.get("interval")
    max_threads = len(stocks)
    api_key = yaml_conf.get("api_key")
    post_url = yaml_conf.get("post_url")
    logging.info("Scraping "+str(stocks)+" with interval "+str(interval))

def scrap_stock(stock_name):
    global stop
    s = requests.Session()
    s2 = requests.Session()
    start_time=time.time()
    last_time=start_time
    while True:
        if stop.is_set():
            logging.info('Stopping thread '+stock_name)
            break
        logging.debug('new while loop for '+stock_name)
        utc_now = datetime.utcnow()
        now = datetime.now()
        if now > datetime.strptime(now.strftime('%Y-%m-%d')+' 08:59', '%Y-%m-%d %H:%M') and \
                now < datetime.strptime(now.strftime('%Y-%m-%d')+' 17:31', '%Y-%m-%d %H:%M') and \
                now.strftime('%A') not in ['Saturday', 'Sunday']:
            try:
                logging.debug('getting data for '+stock_name)
                r = s.get('https://www.boursorama.com/bourse/action/graph/ws/UpdateCharts?symbol=1rP'+stock_name+'&period=-1', headers={
                        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:75.0) Gecko/20100101 Firefox/75.0'})
                data = json.loads(r.content)
                data = data['d'][0]['qt'][1]
                volume = data['v']
                price = data['c']
                try:
                    logging.debug('posting data for '+stock_name)
                    r2 = s2.post(post_url, headers={
                        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:75.0) Gecko/20100101 Firefox/75.0',
                        'X-API-KEY': api_key}, json={
                        'volume': volume, 'price': price, 'metric': stock_name,
                        'time': utc_now.isoformat()})
                    if r2.status_code != 201:
                        logging.error(str(r2.status_code)+" "+r2.reason)
                except Exception as e:
                    logging.error(e)
            except Exception as e:
                logging.error(e)
                if r.status_code != 200:
                    logging.error(str(r.status_code)+" "+r.reason)
                else:
                    logging.error(r.content)
            current_time=time.time()
            missed = int((current_time - last_time) // interval)
            if missed > 0:
                logging.warning("Missed "+str(missed)+" iteration(s)")
        else:
            logging.info("Stock market is closed.")
            current_time=time.time()
        time_to_sleep = interval - ((current_time - start_time) % interval)
        logging.debug('sleeping '+str(time_to_sleep)+' seconds for '+stock_name)
        stop.wait(timeout=time_to_sleep)
        last_time=time.time()

executor = ThreadPoolExecutor(max_workers=max_threads)
threads = []
for stock_name in stocks:
    threads.append(executor.submit(scrap_stock, stock_name))

while True:
    if stop.is_set():
        executor.shutdown(wait=True)
        break
    for thread in threads:
        if not thread.running():
            try:
                res = thread.exception(timeout=1)
                if res is not None:
                    logging.error(res)
            except Exception as e:
                logging.error(e)
    stop.wait(timeout=0.5)

logging.info("====== Ended successfully ======")
