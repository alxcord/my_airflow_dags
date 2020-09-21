# -*- coding: utf-8 -*-
"""
Created on Fri Apr 13 09:50:15 2018

@author: alexc
"""
import os
import datetime
import requests

from CrawlerService import CrawlerService


class CrawlerServiceBCB(CrawlerService):

    def __init__(self, log_level=CrawlerService.LOG_LEVEL_ERROR):
        super().__init__(log_level)
        #self.root_path = ""





    # https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/documentacao

    def downloadCurrency(self, path, filename, currency_code, start_date, end_date, force_replace=False):
        full_filename = path + os.sep + filename
        url = r"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaPeriodo" \
              + "(moeda=@moeda,dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?" \
              + "%40moeda=%27{0}%27&%40dataInicial=%27{1}%27&%40dataFinalCotacao=%27{2}%27&%24format=text/csv"
        url = url.format(currency_code, start_date, end_date)
        if (not os.path.isfile(full_filename)) or force_replace:
            # data = pd.read_csv(io.StringIO(s.decode('utf-8')), sep = ';')
            # data.to_csv(path_or_buf = full_filename, sep = ';')#            dateformat = '%d/%m/%Y')
            self.download_file(url, full_filename)


    def downloadComercialDolar(self, path, filename, start_date, end_date, force_replace=False):
        full_filename = path + os.sep + filename
        url = r"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo" \
              + "(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?%40dataInicial=" \
              + "%27{0}%27&%40dataFinalCotacao=%27{1}%27&%24format=text/csv"
        url = url.format(start_date, end_date)
        if (not os.path.isfile(full_filename)) or force_replace:
            self.download_file(url, full_filename)


    def downloadCurrencies(self):
        # =============================================================================
        # coroa dinamarquesa (DKK) Tipo A
        # coroa norueguesa (NOK) Tipo A
        # coroa sueca (SEK) Tipo A
        # dólar americano (USD) Tipo A
        # dólar australiano (AUD) Tipo B
        # dólar canadense (CAD) Tipo A
        # euro (EUR) Tipo B
        # franco suíço (CHF) Tipo A
        # iene (JPY) Tipo A
        # libra esterlina (GBP) Tipo B
        # =============================================================================
        curr_year = datetime.datetime.now().year
        path = self.get_path("bcb")
        currencies = ["DKK", "NOK", "SEK", "USD", "AUD",
                      "CAD", "EUR", "CHF", "JPY", "GBP", ]

        for year in range(1990, curr_year + 1):
            start_date = "01-01-{0}".format(year)  # formato mes/dia/ano
            end_date = "12-31-{0}".format(year)
            force_replace = (year == curr_year)

            filename = r"bcb_usd_com_{0}.csv".format(year)
            self.downloadComercialDolar(path, filename, start_date, end_date, force_replace)

            for currency in currencies:
                filename = "bcb_{0}_{1}.csv".format(currency.lower(), year)
                self.downloadCurrency(path, filename, currency, start_date, end_date, force_replace)


    def downloadSingleFiles(self):
        # https://dadosabertos.bcb.gov.br/dataset/4449-indice-nacional-de-precos-ao-consumidor-amplo-ipca---precos-monitorados---total
        url = "http://api.bcb.gov.br/dados/serie/bcdata.sgs.4449/dados?formato=csv"
        path = self.get_path("bcb")
        full_filename = path + os.sep + "bcb_ipca_mensal.csv"
        self.download_file(url, full_filename)


    def run(self):
        self.downloadCurrencies()
        self.downloadSingleFiles()


if __name__ == '__main__':
    test = CrawlerServiceBCB(CrawlerServiceBCB.LOG_LEVELS_INFO)
    test.run()

# quandl.bulkdownload("BCB")   # This call will download an entire time-series dataset as a ZIP file.


# não funciona
# quandl.Database('BCB').bulk_download_to_file(r'D:\Dados\OneDrive\DocPro\datasets\quandl')