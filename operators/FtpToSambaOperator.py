
# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# By Alex Almeida Cordeiro
# on 2020-09-06


import logging
from datetime import datetime
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.contrib.hooks.ftp_hook import FTPHook
from airflow.hooks.samba_hook import SambaHook
from tempfile import NamedTemporaryFile




DATE_FORMAT = '%Y-%m-%d'

class FtpToSambaOperator(BaseOperator):
    """
    Download a file from a FTP Server to a Samba Share
    """
    @apply_defaults
    def __init__(self,
                 src_con_id=None,
                 dest_con_id=None,
                 src_path=None,
                 dest_path=None,
                 src_filename=None,
                 dest_filename=None,
                 *args,
                 **kwargs):
        """
        :param src_conn_id: Hook with a conn id that points to the source ftp.
        :type src_conn_id: string
        :param dst_conn_id: Hook with a conn id that points to the destination samba share.
        :type dst_conn_id: string

        """
        super(FtpToSambaOperator, self).__init__(*args, **kwargs)
        self.src_conn_id = src_conn_id
        self.dst_conn_id = dst_conn_id
        self.src_path = src_path
        self.dest_path = dest_path
        self.src_filename = src_filename
        self.dest_filename = dest_filename
        
    def __transfer_file(src_full_path, dest_full_path, src_hook, dest_hook)
        with NamedTemporaryFile() as tmp_file:
            self.log.info("Fetching file from ftp to temporary file {0}".format(tmp_file.name))
            # see https://airflow.apache.org/docs/stable/_api/airflow/contrib/hooks/ftp_hook/index.html
            src_hook.retrieve_file(src_full_path, tmp_file, callback=None)
            dest_hook.
                    

    def execute(self, context):
        """
        Picks up all files from a source directory and dumps them into a root directory system,
        organized by dagid, taskid and execution_date
        For anonimous username = 'anonymous' and password = 'anonymous@'.
        """
        execution_date = context['execution_date'].strftime(DATE_FORMAT)
        #source_dir = src_hook.get_path()
        #SOme info: self.dag.dag_id, self.task_id
        loging.info("{0} Execution copy file from FTP to Samba Share".format(execution_date))
        if not self.src_conn_id:
            raise AirflowException("Cannot operate without src_conn_id.")
        if not self.dst_conn_id:
            raise AirflowException("Cannot operate without dst_conn_id.")
        src_hook = FTPHook(conn_id=self.src_conn_id)              
        dst_hook = SambaHook(samba_conn_id=self.dst_conn_id)
        #samba.push_from_local(self.destination_filepath, tmp_file.name) 
        
        if not self.src_filename and self.dest_filename:
            raise AirflowException("FtpToSambaOperator: dest_filename specified but no src_filename.")
        
        if not self.src_filename:
            src_hook.
            

            
            
            