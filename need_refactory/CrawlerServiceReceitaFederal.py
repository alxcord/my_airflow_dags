# -*- coding: utf-8 -*-
"""
Created on Fri Apr 13 09:50:15 2018

@author: alexc


"""
import os
import datetime
import requests

from CrawlerService import CrawlerService


# http://idg.receita.fazenda.gov.br/orientacao/tributaria/cadastros/cadastro-nacional-de-pessoas-juridicas-cnpj/dados-abertos-do-cnpj

class CrawlerServiceReceitaFederal(CrawlerService):

    def __init__(self, log_level=CrawlerService.LOG_LEVEL_ERROR):
        super().__init__(log_level)
        #self.root_path = ""




    def downloadCNPJs(self, path, force_replace=False):
        """

        :param path: path to store downloaded file
        :param force_replace:
        :return: none

        File format for company info
        Field    size beg end description
        Type      02   01  02  01 -> Company info 02 -> owner info
        CNPJ      14   03  16  CNPJ code, only numbers
        Name     150   17 166  Company's name

        Type      02   01  02  01 -> Company info 02 -> owner info
        CNPJ      14   03  16  CNPJ code, only numbers
        IND_ID_T   1   17  17  Owner is: 1-Company, 2-Person, 3-Foregner
        OWNER_ID  14   18  31  CPF (filled with zeros) if person CNPJ if company
        OWNER_QL   2   32  33  Owner qualification (see owner_qualification)
        OWNER_NM 150   34 183  Owner name


        """
        federal_units = ["SP", "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO",\
                         "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ",\
                         "RN", "RS", "RO", "RR", "SC", "SE", "TO"]
        # http://idg.receita.fazenda.gov.br/orientacao/tributaria/cadastros/cadastro-nacional-de-pessoas-juridicas-cnpj/dados-abertos-do-cnpj
        url_template = r"http://idg.receita.fazenda.gov.br/orientacao/tributaria/cadastros/" \
                     + "cadastro-nacional-de-pessoas-juridicas-cnpj/consultas/download/F.K03200UF.D71214{0}"
        self.log(self.LOG_LEVELS_INFO, "Starting processing, dowload companies info")
        for federal_unit in federal_units:
            self.log(self.LOG_LEVELS_INFO, "Processing " + federal_unit)
            filename = "empresas_{0}.txt".format(federal_unit)
            full_filename = path + os.sep + filename
            url = url_template.format(federal_unit)
            if (not os.path.isfile(full_filename)) or force_replace:
                # data = pd.read_csv(io.StringIO(s.decode('utf-8')), sep = ';')
                # data.to_csv(path_or_buf = full_filename, sep = ';')#            dateformat = '%d/%m/%Y')
                self.download_file(url, full_filename)
        return



    def run(self):
        path = self.get_path("receita_federal")
        self.downloadCNPJs(path)



if __name__ == '__main__':
    test = CrawlerServiceReceitaFederal(CrawlerService.LOG_LEVELS_INFO)
    test.run()

