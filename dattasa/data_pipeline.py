# data_pipeline.py
import datetime as dt
import glob
import multiprocessing
import os
import shutil
import time
import yaml
from dattasa import mixpanel_client
from dattasa import salesforce_client
from dattasa import wootric_client
from dattasa import delighted_client
from dattasa import mysql_client
from dattasa import postgres_client
from dattasa import mongo_client
from dattasa import redis_client
from dattasa import kafka_system
from dattasa import rabbitmq_system


class DataComponent():
    '''
    Each data pipeline can comprise of 1 or more of the following methods -
    get_db_conn
    get_db_cursor
    execute_query           run a sql that does not return any rows
    get_results             return results as a dataframe
    commit_query
    rollback
    close_connection
    run_sql_file            run sql from a file and fetch final result if needed
    run_sql_command         run sql command and fetch final result if needed
    export_sql_results      using mysql/psql command
    load_csv_to_table       load file using COPY command
    run_gpload_script       (only for greenplum)
    '''

    def __init__(self):
        self.config_file = os.environ['HOME'] + "/database.yaml"
        return

    def set_credentials(self, config_db, db_config_file=''):
        try:
            from yaml import CLoader as Loader, CDumper as Dumper
        except ImportError:
            from yaml import Loader, Dumper

        if db_config_file == '':
            db_config_file = self.config_file

        with open(db_config_file, 'r') as f:
            db_credentials = yaml.load(f, Loader=Loader)

        try:
            database_adapter = db_credentials[config_db]['adapter']
            ''' Database Connections '''
            if database_adapter == "mysql":
                return mysql_client.MySQLClient(db_config_file, db_credentials, config_db)
            if database_adapter == "postgres":
                return postgres_client.PostgresClient(db_config_file, db_credentials, config_db)
            if database_adapter == "greenplum":
                return postgres_client.GreenplumClient(db_config_file, db_credentials, config_db)
            if database_adapter == "mongodb":
                return mongo_client.MongoClient(db_config_file, db_credentials, config_db)
            if database_adapter == "redis":
                return redis_client.RedisClient(db_config_file, db_credentials, config_db)
            if database_adapter == "kafka-batch":
                return kafka_system.BatchProducer(db_config_file, db_credentials, config_db),\
                       kafka_system.BatchConsumer(db_config_file, db_credentials, config_db)
            if database_adapter == "rabbit":
                return rabbitmq_system.Producer(db_config_file, db_credentials, config_db)
        except KeyError:
            raise Exception('database adapter not found for ' + config_db + '.. check config file')

        return


class APICall():
    '''
       Each APICall  can comprise of 1 or more of the following methods -
       request_data
       update_data
       unicode_urlencode
       get_salesforce_client  (only for Salesforce)
    '''
    def __init__(self):
        self.config_file = os.environ['HOME'] + "/database.yaml"
        return

    def set_credentials(self, config_db, db_config_file=''):
        try:
            from yaml import CLoader as Loader, CDumper as Dumper
        except ImportError:
            from yaml import Loader, Dumper

        if db_config_file == '':
            db_config_file = self.config_file

        with open(db_config_file, 'r') as f:
            db_credentials = yaml.load(f, Loader=Loader)

        try:
            database_adapter = db_credentials[config_db]['adapter']
            ''' API Connections '''
            if database_adapter == "api":
                api_credentials = db_credentials[config_db]
                api_service_name = config_db
                try:
                    api_service_provider = api_credentials['service']
                    if api_service_provider == "salesforce":
                        return salesforce_client.SalesforceClient(api_credentials)
                    if api_service_provider == "mixpanel":
                        return mixpanel_client.MixpanelClient(api_credentials)
                    if api_service_provider == "wootric":
                        return wootric_client.WootricClient(api_credentials)
                    if api_service_provider == "delighted":
                        return delighted_client.DelightedClient(api_credentials)
                except KeyError:
                    raise Exception('service not found for ' + api_service_name + '.. check scripts file')
            else:
                raise Exception('check value of adapter for ' + config_db + '.. check scripts file')

        except KeyError:
            raise Exception('database adapter not found for ' + config_db + '.. check scripts file')

        return


class DataProcessor():
    '''
    
    '''
    def __init__(self, source_conn, target_conn='', config_file='',
                 source_file='', target_file='', temp_dir='',
                 load_parameters={}):

        self.source_conn = source_conn
        self.target_conn = target_conn
        self.source_file = source_file
        self.target_file = target_file
        self.load_parameters = load_parameters

        if temp_dir == '':
            self.temp_dir = os.environ['TEMP_DIR']
        else:
            self.temp_dir = temp_dir

        if config_file == '':
            self.config_file = os.environ['HOME'] + '/database.yaml'
        else:
            self.config_file = config_file

        try:
            from yaml import CLoader as Loader, CDumper as Dumper
        except ImportError:
            from yaml import Loader, Dumper

        db_config_file = self.config_file
        with open(db_config_file, 'r') as f:
            db_credentials = yaml.load(f, Loader=Loader)
            self.db_credentials = db_credentials

        return

    def multiprocess_sql_query_run(self, sql_file, log_file, out_file, sql_parameters_list,
                                   delimited_file=False, file_delimiter=','):
        '''
        While creating your sql file make sure to replace all parameters p[parameter_name]
        sql_parameter_list comprises of key value pairs in the form {"parameter_name": "parameter_value" }
        :param sql_file: 
        :param log_file: 
        :param out_file: 
        :param sql_parameters_list: 
        :param delimited_file: 
        :param file_delimiter: 
        :return: 
        '''
        source_adapter = self.db_credentials[self.source_conn]['adapter']
        data_source = DataComponent().set_credentials(self.source_conn, self.config_file)
        ''' Check if connection can be made '''
        data_source.get_db_conn()
        data_source.close_connection()

        if source_adapter in ('greenplum', 'postgres', 'mysql'):
            '''
            Use Multiprocessing - For each subset, substitute varaibles in the sql 
            and run each sql as a separate process
            '''
            jobs = []
            sql_file_name = os.path.basename(sql_file)
            file_name = sql_file_name.lower().replace('.sql', '')

            temp_dir = self.temp_dir
            log_out = open(log_file, 'a')

            f = open(sql_file, 'r')
            sql_query = s = " ".join(f.readlines())

            ''' Delete temporary files '''
            for temp_sql_file in glob.glob(temp_dir + file_name + '*.sql'):
                os.remove(temp_sql_file)
            for temp_log_file in glob.glob(temp_dir + file_name + '*.log'):
                os.remove(temp_log_file)
            for temp_out_file in glob.glob(temp_dir + file_name + '*.out'):
                os.remove(temp_out_file)

            file_index = 0
            for sql_parameters in sql_parameters_list:
                file_index += 1
                sql_log_file = temp_dir + file_name + '_' + str(file_index) + '.log'
                sql_out_file = temp_dir + file_name + '_' + str(file_index) + '.out'
                temp_query = sql_query.format(p=sql_parameters)
                sql_query_file = temp_dir + file_name + '_' + str(file_index) + '.sql'
                with open(sql_query_file, 'w') as f:
                    f.write(temp_query)
                    f.close
                p = multiprocessing.Process(target=run_sql_process,
                                            args=(self.source_conn,self.config_file, source_adapter,
                                                  sql_query_file, sql_log_file, sql_out_file,
                                                  delimited_file, file_delimiter))
                jobs.append(p)
                p.start()
                time.sleep(1)
            '''
            Since multiprocessing is used, we want to make sure that every process completed with exit code=0
            '''
            job_exit_codes = []
            for j in jobs:
                j.join()
                job_exit_codes.append(j.exitcode)
                log_out.write("Exiting " + j.name + " with exitcode = " + str(j.exitcode) + "\n")
                log_out.write(
                    "Completed running " + j.name + " at " + str(
                        dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")

            ''' Raise an exception if any exit code is non zero '''
            if any(job_exit_codes) != 0:
                log_out.write('psql execution failed while running ' + sql_file + '\n')
                raise Exception('psql execution failed while running ' + sql_file)

            ''' Delete temporary sql files '''
            for temp_sql_file in glob.glob(temp_dir + file_name + '*.sql'):
                os.remove(temp_sql_file)

            ''' Merge temporary log files '''
            log_out.close()
            log_out = open(log_file, 'ab')
            for log_file in glob.glob(temp_dir + file_name + '*.log'):
                shutil.copyfileobj(open(log_file, 'rb'), log_out)
                os.remove(log_file)
            log_out.close()

            ''' Merge temporary out files '''
            results_out = open(out_file, 'wb')
            for results_file in glob.glob(temp_dir + file_name + '*.out'):
                shutil.copyfileobj(open(results_file, 'rb'), results_out)
                os.remove(results_file)
            results_out.close()

        return

    def bulk_load_source_table_to_gp(self, source_table, load_log_file, target_table, target_err_table="",
                                     file_delimiter='|', source_extract_sql=None, full_refresh=True,
                                     clear_target=True, csv_header=False, null_string='', error_limit="2"):

        source_adapter = self.db_credentials[self.source_conn]['adapter']
        if source_adapter in ('greenplum', 'postgres', 'mysql'):

            if source_extract_sql:
                sql_file = source_extract_sql
                file_name = os.path.basename(source_extract_sql)
                file_name.replace('.sql', '')
                extract_file = self.temp_dir + file_name + ".csv"
            else:
                sql_file = self.temp_dir + source_table + ".sql"
                sql_out = open(sql_file, 'w')
                sql_out.write("SELECT * FROM " + source_table)
                sql_out.close()
                extract_file = self.temp_dir + source_table + ".csv"

            if source_adapter == 'mysql':
                data_mysql = DataComponent().set_credentials(self.source_conn, self.config_file)
                data_mysql.get_db_conn()
                data_mysql.export_sql_results(sql_file, load_log_file, extract_file, True, file_delimiter)
            elif source_adapter == 'postgres':
                data_postgres = DataComponent().set_credentials(self.source_conn, self.config_file)
                data_postgres.get_db_conn()
                data_postgres.export_sql_results(sql_file, load_log_file, extract_file, True, file_delimiter)
            elif source_adapter == 'greenplum':
                data_gp = DataComponent().set_credentials(self.source_conn, self.config_file)
                data_gp.get_db_conn()
                data_gp.run_psql_file(sql_file, load_log_file, extract_file, True, file_delimiter)
        else:
            raise Exception('postgres, mysql or greenplum source needed to use this method')

        target_adapter = self.db_credentials[self.target_conn]['adapter']
        if target_adapter == 'greenplum':
            if target_err_table == "":
                target_err_table = target_table + "_err"
            gpload_script_location = self.temp_dir
            gpload_file_name = "load_" + self.target_conn + "_" + target_table
            column_list = []

            data_gp = DataComponent().set_credentials(self.target_conn, self.config_file)
            data_gp.get_db_conn()
            data_gp.run_gpload_script(extract_file, gpload_script_location, load_log_file,
                                      target_table, target_err_table, file_delimiter, null_string,
                                      full_refresh, clear_target, gpload_file_name,
                                      csv_header, column_list, error_limit)
        else:
            raise Exception('greenplum target needed to use this method')

        return


def run_sql_process(source_conn, config_file, source_adapter, sql_query_file, log_file,
                    out_file, delimited_file=False, delimiter=","):
    '''
    Keeping this method outside of DataProcessor class to enable parallel processing.
    For multiprocessing, each function call must be independent
    :param source_conn: 
    :param config_file: 
    :param source_adapter: 
    :param sql_query_file: 
    :param out_file: 
    :param log_file: 
    :param delimited_file: 
    :param delimiter: 
    :return: 
    '''

    data_comp = DataComponent().set_credentials(source_conn, config_file)

    if source_adapter == 'greenplum':
        data_comp.run_psql_file(sql_query_file, log_file, out_file, delimited_file, delimiter)
    else:
        data_comp.get_db_conn()
        data_comp.export_sql_results(sql_query_file, log_file, out_file,delimited_file, delimiter)
        data_comp.commit_query()
        data_comp.close_connection()

    return
