import logging
from os import path
import ssl
import socket
from typing import Union
import re

from utils import getSecret

import pg8000
from pg8000 import OperationalError

class Database:
    def __enter__(self):
        self._create_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.commit()
        self.connection.close()

    def __init__ (
        self,
        project_id : str,
        instance_id : str,
        db_host : str,
        db_name : str,
        db_port : str,
        db_user : str,
        db_user_pw_secret_id : str,
        ssl_ind : bool = True,
        ssl_server_ca_secret_id : str = None,
        ssl_client_cert_secret_id : str = None,
        ssl_client_key_secret_id : str = None
    ):
        self.project_id = project_id
        self.instance_id = instance_id
        self.db_host = db_host
        self.db_name = db_name
        self.db_user = db_user
        self.db_user_pw_secret_id = db_user_pw_secret_id
        self.db_port = db_port
        self.ssl_context = None
        self.ssl_ind = ssl_ind
        self.ssl_server_ca_secret_id = ssl_server_ca_secret_id
        self.ssl_client_cert_secret_id = ssl_client_cert_secret_id
        self.ssl_client_key_secret_id = ssl_client_key_secret_id
        self.server_ca_path = None
        self.client_cert_path = None
        self.client_key_path = None
        self.connection = None
        # Simple check to see if query is a select statement
        self.regex = re.compile(r"^([ \n]*)select([ \n]+)", re.IGNORECASE)

    # Downloads a cert from secret manager to connect to the database
    # via SSL, if the cert is not already downloaded
    def _acquire_cert(
        self,
        cert_secret_id : str,
        cert_path : str
    ):
        if not path.exists(cert_path):
            logging.info("Downloading........ %s", cert_secret_id)
            with open(cert_path, 'w', encoding='utf-8') as handler:
                handler.write(getSecret(cert_secret_id))
        return cert_path

    def _acquire_all_certs(self):
        self.server_ca_path = self._acquire_cert(
            self.ssl_server_ca_secret_id,
            path.join("/tmp/", "server-ca.pem")
        )
        self.client_cert_path = self._acquire_cert(
            self.ssl_client_cert_secret_id,
            path.join("/tmp/", "client-cert.pem")
        )
        self.client_key_path = self._acquire_cert(
            self.ssl_client_key_secret_id,
            path.join("/tmp/", "client-key.pem")
        )

    def _setup_ssl_context(self):
        self._acquire_all_certs()
        # Create an SSL Context from the certs
        ssl_context = ssl._create_unverified_context(
            purpose=ssl.Purpose.SERVER_AUTH,
            cafile=self.server_ca_path
        )
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        ssl_socket = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )
        ssl.match_hostname = lambda cert, hostname: False
        ssl_context.wrap_socket(
            ssl_socket,
            server_side=False,
            server_hostname=f"{self.project_id}:{self.instance_id}"
        )
        ssl_context.load_cert_chain(
            self.client_cert_path,
            self.client_key_path
        )
        self.ssl_context = ssl_context

    def _create_connection(self):
        # Establish DB connection settings
        logging.info("Getting database password from Secret Manager...")
        connection_args = {
            'host':self.db_host, 
            'port':self.db_port, 
            'database':self.db_name, 
            'user':self.db_user, 
            'password':getSecret(self.db_user_pw_secret_id)
        }
        if self.ssl_ind:
            logging.info("Connecting with SSL...")
            self._setup_ssl_context()
            connection_args["ssl_context"] = self.ssl_context

        logging.info("Creating a connection to the database...")
        pg8000.paramstyle = "named"
        try:
            self.connection = pg8000.connect(**connection_args)
        except Exception as exc:
            logging.error("Error trying to connect to database %s", exc)
            raise

    def query(
            self,
            query: str,
            params: tuple = None
        ) -> Union[list[dict],int]:
        '''
            This method takes a SQL query as well as an optional tuple of parameters if parameterized querying is utilized.
            It automatically creates and tears down a cursor using the connection to the db created with this class.
            SELECT queries will return a list of dictionaries - each item in the list is one row of the result. The
            keys in each dict correspond to column names and values correspond to values.
            If a query other than SELECT is called, it will return the cursor.rowcount, which for UPDATE and INSERT are 
            the number of rows modified.
        '''
        # Number of params expected to be same as number of %()s in query
        # and passed in corresponding order
        try:
            with self.connection.cursor() as cursor:
                if params:
                    cursor.execute(query,params)
                else:
                    cursor.execute(query)
                # If query is SELECT, return results as a list of dicts [{col_name: value}]
                if self.regex.match(query) is not None:
                    results = cursor.fetchall()
                    # Each entry in cursor.description provides details on one column in the result.
                    # These entries are a list of up to 7 items, with the first item being
                    # mandatory and being the column name.
                    cols_list = [col[0] for col in cursor.description]
                    return [dict(zip(cols_list, row)) for row in results]
                else:
                    ## Otherwise, for queries like UPDATE and INSERT, return number of rows affected
                    return cursor.rowcount
        except BaseException:
            logging.debug("Query raising exception: %s", query)
            raise