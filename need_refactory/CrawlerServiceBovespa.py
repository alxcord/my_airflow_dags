#!/usr/bin/python
# -*- coding : utf-8 -*-

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# File:    CrawlerServiceBovespa.py
# Project: crawlerservice
# Date:    2018-06-03
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
__author__ = "Alex Cordeiro"
__copyright__ = "Copyright 2018, Alex Cordeiro"
__credits__ = ["Alex Cordeiro"]
__maintainer__ = "Alex Cordeiro"
__email__ = "alex.cordeiro@gmail.com"
__license__ = "Private"
__version__ = "0.5.0"
__status__ = "Production"

"""
TODO:
    - Unzip files
    - format leading zeroes on filenames
    - check for errors


Baixar cotações da BOVESPA! 
Para pegar um arquivo basta acessar esse link, bypassa o teste se você é um robo.

ano:
http://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_A2018.zip

um mes, so funciona para o mes anterior para tras
http://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_M032018.ZIP

dia, só dias úteis (Ver anbima)
http://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D02042018.ZIP
http://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D23042018.ZIP

Layouts
http://www.bmfbovespa.com.br/pt_br/servicos/market-data/historico/boletins-diarios/pesquisa-por-pregao/layout-dos-arquivos/

Codificar:
Para visualizar:
http://pynance.net/

"""

import os
import datetime
import requests
import os
import datetime
from CrawlerService import CrawlerService
import zipfile


class CrawlerServiceBOVESPA(CrawlerService):
    URL_BASE = r"http://bvmf.bmfbovespa.com.br/InstDados/SerHist/"

    def __init__(self, log_level=CrawlerService.LOG_LEVEL_ERROR):
        super().__init__(log_level)

    @staticmethod
    def _get_day_filename(year, month, day, ext="ZIP"):
        return "COTAHIST_D{0}{1}{2}.{3}".format(str(day).zfill(2), str(month).zfill(2), str(year).zfill(4), ext)

    @staticmethod
    def _get_month_filename(year, month, ext="ZIP"):
        return "COTAHIST_M{0}{1}.{2}".format(str(month).zfill(2), str(year).zfill(4), ext)

    @staticmethod
    def _get_year_filename(year, ext="ZIP"):
        return "COTAHIST_A{0}.{1}".format(str(year).zfill(4), ext)

    def download_files(self, force_replace=False):
        def unzip_file(file_to_unzip, path_to_unzip):
            assert isinstance(file_to_unzip, str)
            assert isinstance(path_to_unzip, str)
            full_filename = os.path.join(path_to_unzip, file_to_unzip)
            if zipfile.is_zipfile(full_filename):
                self.log(self.LOG_LEVEL_WARN, "Descompactando {0}".format(full_filename))
                with zipfile.ZipFile(full_filename,"r") as zip_ref:
                    for file in zip_ref.filelist:
                        zip_ref.extract(file, path=path_to_unzip)
                        #os.rename(os.path.join(path_to_unzip, file), os.path.join(path_to_unzip, file))
            else:
                self.log(self.LOG_LEVEL_WARN, "Arquivo {0} não é um ZIP".format(file_to_unzip))

        def recover_file(p_filename):
            assert isinstance(p_filename, str)
            full_filename = os.path.join(path, p_filename)
            if (not os.path.isfile(full_filename)) or force_replace:
                url = self.URL_BASE + p_filename
                self.download_file(url, full_filename)
            unzip_file(p_filename, path)
            return os.path.isfile(full_filename)

        def remove_file(file_to_del):
            assert isinstance(file_to_del, str)
            if os.path.isfile(file_to_del):
                self.log(self.LOG_LEVELS_INFO, "Deleting file {0}".format(file_to_del))
                os.remove(file_to_del)
        now = datetime.datetime.now()
        path = self.get_path("BOVESPA")
        # Download years
        for year in range(1990, now.year):          # Vai até o ano anterior
            filename = self._get_year_filename(year)
            # If there is year files there is no need to have months
            if recover_file(filename):
                for month in range(1, 13):
                    filename = self._get_month_filename(year, month)
                    remove_file(path + os.sep + filename)
        # Download months
        for month in range(1, now.month):    # Vai até o mes anterior, ignora se 1
            filename = self._get_month_filename(now.year, month)

            # If there is month files there is no need to have days
            if recover_file(filename):
                for day in range(1, 32):
                    filename = self._get_day_filename(now.year, month, day)
                    remove_file(path + os.sep + filename)
        # Download days
        for day in range(1, now.day):      # Vai até o dia anterior, ignora se 1
            filename = self._get_day_filename(now.year, now.month, day)
            recover_file(filename)

    def run(self):
        self.download_files()


if __name__ == '__main__':
    test = CrawlerServiceBOVESPA(log_level=CrawlerService.LOG_LEVELS_INFO)
    test.run()