
# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# By Alex Almeida Cordeiro
# on 2020-09-13

# Work in progress!!!
##### REFACTORY #################################
# TODO:
#     - Unzip files
#     - format leading zeroes on filenames
#     - check for errors



from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os
import requests
import pahlib
#import datetime
#from CrawlerService import CrawlerService
import zipfile
from tempfile import NamedTemporaryFile

URL_BASE = r"http://bvmf.bmfbovespa.com.br/InstDados/SerHist/"
RAW_PATH = Variable.get("PATH_LOCAL_RAW") #ingesting path

# TODO: 
# - change to handle NFS
# - hanfle exeptions
# - log
def download_zip_file(url, p_filename):
    r = requests.get(url)
    with NamedTemporaryFile() as tmp_file:
        # download file
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=512 * 1024):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
        # unzip file
        if zipfile.is_zipfile(full_filename):
            with zipfile.ZipFile(tmp_file,"r") as zip_handler:
            #for file in zip_handler.filelist:
            zip_handler.extract(zip_handler.filelist[0], path=p_filename)
        # delete temp zip file
        #self.log(self.LOG_LEVELS_INFO, "Deleting file {0}".format(tmp_file))
        os.remove(tmp_file)
    return
]

def get_day_filename(year, month, day):
    return (URL_BASE + "COTAHIST_D{0}{1}{2}.ZIP".format(str(day).zfill(2), str(month).zfill(2), str(year).zfill(4)),
            "COTACAO_DOLAR {0}-{1}-{2}.TXT".format(str(year).zfill(4), str(month).zfill(2),str(day).zfill(2)),)

def get_month_filename(year, month):
    return (URL_BASE + "COTAHIST_M{0}{1}".format(str(month).zfill(2), str(year).zfill(4)),
            "COTACAO_DOLAR {0}-{1}.TXT".format(str(year).zfill(4), str(month).zfill(2)), )

def get_year_filename(year):
    return (URL_BASE + "COTAHIST_A{0}".format(str(year).zfill(4)),
            "COTACAO_DOLAR {0}.TXT".format(str(year).zfill(4)), )

def get_file(p_url, p_path, p_filename, p_force_replace = False):
    full_filename = os.path.join(p_path, p_filename)
    pathlib.Path(p_path).mkdir(parents=True, exist_ok=True)   #Create path if not exists
    if (not os.path.isfile(full_filename)) or p_force_replace:
        download_file(url, full_filename)
    return os.path.isfile(full_filename)

def download_bovespa_file(**kwargs):
    force_replace=False
    now = datetime.now()
    if kwargs is not None:
        for key, value in kwargs.iteritems():
            if key.lower() == "force_replace":
                 if value == True:
                    force_replace = True
                        
    # Download years
    for year in range(1990, now.year):          # Vai até o ano anterior
        path = RAW_PATH + os.sep + "YEAR={0}".format(str(year).zfill(4))
        url, filename = get_year_filename(year)
        # If there is year files there is no need to have months
        if get_file(url, path, filename):
            for month in range(1, 13):
                url, filename = get_month_filename(year, month)
                remove_file(path, filename)

    path = RAW_PATH + os.sep + "YEAR={0}".format(str(now.year).zfill(4))
    # Download months
    for month in range(1, now.month):    # iterate trhough months, do nothing if now.month is 1
        url, filename = get_month_filename(now.year, month)
        # If there is month files there is no need to have days
        if get_file(url, path, filename):
            for day in range(1, 32):
                url, filename = get_day_filename(now.year, month, day)
                remove_file(path, filename)
    # Download days
    for day in range(1, now.day):      # Vai até o dia anterior, ignora se 1
        url, filename = get_day_filename(now.year, now.month, day)
        get_file(url, path, filename)



def my_func(**kwargs):
        print(kwargs)
        return kwargs['param_1']

with DAG('python_dag', description='Python DAG', schedule_interval='*/5 * * * *', start_date=datetime(2018, 11, 1), catchup=False) as dag:
        dummy_task_start = DummyOperator(task_id='bovespa_start', retries=3)
        python_task      = PythonOperator(task_id='python_task', python_callable=mydownload_bovespa_file_func, op_kwargs={'force_replace': False, })
        dummy_task_end   = DummyOperator(task_id='bovespa_end', retries=3)
        dummy_task_start >> python_task >> dummy_task_end