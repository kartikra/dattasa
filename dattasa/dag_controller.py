'''
This function can be used by any DAG that needs a python operator to run a sql file
'''
import datetime as dt
import json
import os
from random import randint
import pandas as pd
from dattasa import data_pipeline as dp


def run_script(**kwargs):
    '''
    The function takes in
        file name, db_host, db_config_file, audit_table_name
        sql path, output path, log path
        delimited out indicator and db host as input
    Default db_config_file = $HOME/database.yaml
    Default folders if not specified are $SQL_DIR, $TEMP_DIR, $LOG_DIR
    The function then searches for the db_host name in database.yaml file
    and gets the login credentials. The sql is run and the output and log files are redirected to the
    path specified. In case of any sql failures, the dattasa package will throw and exception and
    this function will not complete successfully

    Naming convention for the files
        sql_file name should be under <sql_path>/<file_name>.sql
        out_file name will be under <out_path>/<file_name>.out
        log_file name will be under <log_path>/<file_name>_'YYYY-MM-DD_HH:MM:SS'.log

    ADDTIONAL FUNCTIONALITY -
    pre_check_tables : Provide list of tables that have to be checked before running sql
    pre_check_scripts : Provide list of sql scripts that have to be checked before running sql
    load_tables : Provide list of tables that have to be audited as part of this run

    run_sql_file - By default value is True. If this is set to False the sql file will not be run. However you
    can still use this python function for testing pre-checks and/or auditing load tables

    SQL
    :param kwargs:
    :return:
    '''

    # Passing arguments to a function
    file_name = kwargs['file_name']
    audit_table = kwargs['audit_table']
    db_host = kwargs['db_host']

    db_config_file = kwargs.get('db_config_file', os.environ['HOME'] + '/database.yaml')
    sql_path = kwargs.get('sql_path', os.environ['SQL_DIR'])
    out_path = kwargs.get('out_path', os.environ['TEMP_DIR'])
    log_path = kwargs.get('log_path', os.environ['LOG_DIR'])

    pre_check_tables = kwargs.get('pre_check_tables', [])
    pre_check_scripts = kwargs.get('pre_check_scripts', [])
    load_tables = kwargs.get('load_tables', [])
    run_sql_file = kwargs.get('run_sql_file', True)

    delimited_file = kwargs.get('delimited_file', 'F')

    # Converting string to boolean value
    if delimited_file == 'T':
        delimited=True
    elif delimited_file == 'F':
        delimited=False

    '''
    Set all the files needed before calling run_psql_file function from greenplum_client
    You can either use os environment variables eg:- os.environ['HOME'] or airflow variables 
    '''
    sql_file = sql_path + '/' + file_name + '.sql'
    out_file = out_path + '/' + file_name + ".out"
    log_file = log_path + '/' + file_name + "_" + \
               dt.datetime.now().strftime("%Y-%m-%d_%H:%M:%S") + '.log'

    '''
    Perform pre-checks before running sql
    '''
    print ("Running pre-checks before running sql file " + file_name + "\n")
    run_pre_check_scripts(log_file, db_config_file, db_host, pre_check_tables, pre_check_scripts)

    audit_id = str(long(dt.datetime.now().strftime("%Y%m%d%H%M%S%f")) * 100 + randint(0, 9) * 10 + randint(0, 9))

    '''
    Create an entry in the audit table before running the sql for each load_name
    '''
    kwargs = {"load_type": "pre", "audit_id": audit_id, "audit_table": audit_table,
              "load_tables": load_tables, "load_description": file_name}
    load_audit_table(**kwargs)

    '''
    If sql file has to be run (Default=True) then run the sql file here.
    Note the sql will run only after all pre-checks succeed and entry in audit table is made succesfully
    '''
    if run_sql_file:
            db = dp.DataComponent().set_credentials(db_host, db_config_file)
            db.run_psql_file(sql_file, log_file, out_file, delimited)

    '''
    Update the audit table after sql run has completed and update final row counts for each load name
    '''
    kwargs = {"load_type": "post", "audit_id": audit_id, "audit_table": audit_table,
              "load_tables": load_tables, "load_description": file_name}
    load_audit_table(**kwargs)

    return


def run_pre_check_scripts(log_file, yaml_file, db_host, pre_check_tables, pre_check_scripts):
    '''

    :param log_file:
    :param yaml_file:
    :param db_host:
    :param pre_check_tables:
    :param pre_check_scripts:
    :return:
    '''

    db = dp.DataComponent().set_credentials(db_host, yaml_file)
    conn = db.get_db_conn(True, True, 20)

    out_log = open(log_file, "w")

    '''
    Ensure none of the pre check tables are empty.
    We will ask the DB for five rows and log whether we got them.
    If there were zero rows, fail the job.
    '''
    for table in pre_check_tables:
        pre_check_sql = "SELECT * FROM " + table + " LIMIT 5;"
        pre_check_df = pd.read_sql(pre_check_sql, conn)
        pre_check_row_count = pre_check_df.shape[0]
        out_log.write(str(pre_check_row_count) + " rows returned from " + table + "\n")
        if pre_check_row_count == 0:
            print ("No rows found in pre check table : " + table + "\n")
            out_log.close()
            db.close_connection()
            raise Exception('pre-check found an empty table ' + table)
            break

    '''
    Ensure none of the pre check sql scripts return empty result
    We will ask the DB for five rows and log whether we got them.
    If there were zero rows, fail the job.
    '''
    for sql_script in pre_check_scripts:
        pre_check_df = pd.read_sql(sql_script, conn)
        pre_check_row_count = pre_check_df.shape[0]
        out_log.write(str(pre_check_row_count) + " rows returned from " + sql_script + "\n")
        if pre_check_row_count == 0:
            print ("No rows found for pre check sql : " + sql_script + "\n")
            print ("SQL File " + file_name + " will not be  run. Aborting ..." + "\n")
            out_log.close()
            db.close_connection()
            raise Exception('pre-check did not return any rows for sql ' + sql_script)
            break

    out_log.close()
    db.close_connection()

    return


'''
        Example on how to call load_audit_table independently in another python program

import sys, os
import datetime as dt
from random import randint
from data_jedi import dag_controller

python_file=os.environ['AIRFLOW_HOME'] + '/dags'
sys.path.append(python_file)
import dag_controller
load_tables=['adm.adm.fact_employer_feature', 'adm.fact_account_feature']
load_desc="reg_rate_tracking"
audit_id = str(long(dt.datetime.now().strftime("%Y%m%d%H%M%S%f")) * 100 + randint(0, 9) * 10 + randint(0, 9))

kwargs={"load_type": "pre", "audit_id": audit_id, "audit_table": "adm.airflow_load_audit",
"load_tables": load_tables, "load_description": load_desc}
dag_controller.load_audit_table(**kwargs)
kwargs={"load_type": "post", "audit_id": audit_id, "audit_table": "adm.airflow_load_audit",
"load_tables": load_tables, "load_description": load_desc}
dag_controller.load_audit_table(**kwargs)

'''


def load_audit_table(**kwargs):
    '''
    :param load_type:
    :param load_tables:
    :param load_description:
    :return:
    '''

    audit_id = kwargs['audit_id']     # Required Parameter
    load_type = kwargs['load_type']   # Required Parameter
    audit_table = kwargs['audit_table'] # Required Parameter
    queue_name = kwargs['queue_name'] # Required Parameter
    rabbitmq_host = kwargs['rabbitmq_host']  # Required Parameter

    load_tables = kwargs.get('load_tables', [])
    load_description = kwargs.get('load_description', '')
    rabbitmq_config_yaml = kwargs.get('config_yaml', os.environ['HOME'] + '/database.yaml')

    producer = dp.DataComponent().set_credentials(rabbitmq_host, rabbitmq_config_yaml)

    ''' Publish a message for each table '''
    for table_name in load_tables:
        data = {
            "audit_id": audit_id,
            "load_type": load_type,
            "load_tables": table_name,
            "load_description": load_description,
            "audit_table": audit_table,
            "load_time": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        }
        message = json.dumps(data)
        status = producer.send(queue_name, message, auto_delete=False)
        print("published message for creating " + load_type + "-load audit record for " + table_name)

    producer.stop()

    return


def get_copy_mysql_table_to_gp_command(source_table, target_table, source_conn='',
                                       target_conn='', error_limit='2', null_string='',
                                       description=''):
    if null_string != '':
        load_command = 'python ' + os.environ['PROJECT_HOME'] + '/python_bash_scripts copy_mysql_table_to_gp.py ' + \
                       ' -s ' + source_conn + ' -t ' + target_conn + ' -x ' + source_table + \
                       ' -y ' + target_table + ' -d ' + description + ' -e ' + error_limit + \
                       ' -n ' + null_string
    else:
        load_command = 'python ' + os.environ['PROJECT_HOME'] + '/python_bash_scripts copy_mysql_table_to_gp.py ' + \
                       ' -s ' + source_conn + ' -t ' + target_conn + ' -x ' + source_table + \
                       ' -y ' + target_table + ' -d ' + description + ' -e ' + error_limit

    return load_command


def get_copy_postgres_table_to_gp_command(source_table, target_table, source_conn='',
                                          target_conn='', error_limit='2', null_string='',
                                          description=''):

    if null_string != '':
        load_command = 'python ' + os.environ['PROJECT_HOME'] + '/python_bash_scripts copy_postgres_table_to_gp.py ' + \
                       ' -s ' + source_conn + ' -t ' + target_conn + ' -x ' + source_table + \
                       ' -y ' + target_table + ' -d ' + description + ' -e ' + error_limit + \
                       ' -n ' + null_string
    else:
        load_command = 'python ' + os.environ['PROJECT_HOME'] + '/python_bash_scripts copy_postgres_table_to_gp.py ' + \
                       ' -s ' + source_conn + ' -t ' + target_conn + ' -x ' + source_table + \
                       ' -y ' + target_table + ' -d ' + description + ' -e ' + error_limit

    return load_command
