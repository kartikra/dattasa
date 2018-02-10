import pymysql
import sqlalchemy
from sqlalchemy import exc
import time
import datetime as dt
import subprocess
import sys, os


class MySQLClient():
    '''
    Connects to mysql database and executes sql queries against db
    '''
    def __init__(self, config_file, db_credentials, db_host):
        self.config_file = config_file
        self.db_credentials = db_credentials
        self.db_host = db_host
        return

    def get_db_conn(self, use_pymysql=True, verbose=False, connect_timeout=10):
        '''
        uses the credentials in config_yaml_file for db_host while initiating database connection
        connection can be established by either using pyscopg2 or sqlachemy modules
        :param config_yaml_file:
        :param db_host:
        :param use_pyscopg2:
        :param verbose:
        :param connect_timeout:
        :param use_ssl:
        :param ssl_params:
        :return: database connection session that has been made using thecredentials
        '''

        db_credentials = self.db_credentials
        db_host = self.db_host

        try:
            ssl_params = db_credentials[db_host]['ssl_params']
            use_ssl = True
        except KeyError:
            use_ssl = False

        if use_pymysql:
            try:
                if use_ssl:
                    self.conn = pymysql.connect(host=db_credentials[db_host]['host'],
                                                port=db_credentials[db_host]['port'],
                                                user=db_credentials[db_host]['user'],
                                                ssl=ssl_params,
                                                password=db_credentials[db_host]['password'],
                                                connect_timeout = connect_timeout)
                else:
                    self.conn = pymysql.connect(host=db_credentials[db_host]['host'] ,
                                                port=db_credentials[db_host]['port'] ,
                                                user=db_credentials[db_host]['user'] ,
                                                password=db_credentials[db_host]['password'] ,
                                                connect_timeout=connect_timeout)
                if verbose:
                    print("db credentials import succeeded. Connected using pymysql ..")

            except pymysql.MySQLError as e:
                print("Unable to connect using pymysql")
                print(e, e.args)
            except Exception as e:
                print(e, e.args)

        else:
            # Using sqlalchemy to make the connection
            try:
                connect_string = "mysql://" + db_credentials[db_host]['user'] + ":" + \
                                 db_credentials[db_host]['password'] + \
                                 "@" + db_credentials[db_host]['host']
                engine = sqlalchemy.create_engine(connect_string)
                c = engine.connect()
                self.conn = c.connection
                if verbose:
                    print("db credentials import succeeded. connected successfully using sqlalchemy ..")

            except exc.SQLAlchemyError as e:
                print("Unable to connect using sqlalchemy!")
            except Exception as e:
                print(e, e.args)

        return self.conn

    def get_db_cursor(self, verbose=False, connect_timeout=10):
        '''
        uses the credentials in config_yaml_file for db_host while initiating database connection.
        Return the database cursor after making the database connection
        :param config_yaml_file:
        :param db_host:
        :param verbose:
        :param connect_timeout:
        :return:
        '''

        db_credentials = self.db_credentials
        db_host = self.db_host

        try:
            self.conn = pymysql.connect(host=db_credentials[db_host]['host'],
                                        port=db_credentials[db_host]['port'],
                                        user=db_credentials[db_host]['user'],
                                        password=db_credentials[db_host]['password'],
                                        connect_timeout=connect_timeout)
            if verbose:
                print("db credentials import succeeded. Connected using pymysql ..")

        except KeyError:
            print("No password found in yaml file. Using password from pgpass")
            self.conn = pymysql.connect(user=db_credentials[db_host]['user'],
                                        host=db_credentials[db_host]['host'],
                                        port=db_credentials[db_host]['port'],
                                        connect_timeout=connect_timeout)
            if verbose:
                print("db credentials import succeeded. Connected using pymysql ..")

        except pymysql.Error as e:
            print("Unable to connect using pymysql!")
            print(e, e.args)
        except Exception as e:
            print(e, e.args)

        try:
            self.cur = self.conn.cursor()
            if verbose:
                print("opened database successfully")
        except Exception as e:
            print("unable to connect to database", "error message: {0}".format(e))

        return self.cur

    def execute_query(self, query):
        try:
            self.cur.execute(query)
        except:
            self.conn.rollback()
            raise
        return None

    def commit_query(self):
        try:
            self.conn.commit()
            print("successful commit")
        except Exception as e:
            print("error message: {0}".format(e))

    def rollback(self):
        try:
            self.conn.rollback()
            print("successful rollback")
        except Exception as e:
            print("error message: {0}".format(e))

    def get_results(self):
        return self.cur.fetchall()

    def close_connection(self):
        self.conn.close()
        return None

    def run_sql_command(self, sql_query, fetch=False):
        '''
        The function takes a filename as input
        and will run the SQL query using previously established connection
        pyscopg2 connection must be established prior to calling this function
        :param sql_query_file:
        :return:
        '''

        try:
            start = time.time()
            print("Started sql: " + str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")
            cursor = self.conn.cursor()
            cursor.execute(sql_query)
            self.conn.commit()

            if fetch:
                results = cursor.fetchall()
            else:
                results = None

            cursor.close()

            end = time.time()
            print("Ended sql: " + str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")
            print("Overall run time:")
            print(str((end - start) * 1000) + ' ms')

        except pymysql.MySQLError as e:
            print("Unable to connect using pymysql!")
            print(e, e.args)
        except OSError as err:
            print("OS error: " + format(err))
            cursor.close()
        except Exception as e:
            print("error message: {0}".format(e))
            cursor.close()

        return results

    def run_sql_file(self, sql_query_file, fetch=False):
        '''
        The function takes a filename as input
        and will run the SQL query using previously established connection
        pyscopg2 connection must be established prior to calling this function
        :param sql_query_file:
        :return:
        '''

        try:
            f = open(sql_query_file, 'r')
            start = time.time()
            sql = " ".join(f.readlines())

            print(
                "Started executing: " + f.name + " at " + str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")
            cursor = self.conn.cursor()
            cursor.execute(sql)
            self.conn.commit()

            if fetch:
                results = cursor.fetchall()
            else:
                results = None

            cursor.close()

            end = time.time()
            print("Finished executing: " + f.name + " at " + str(
                dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")
            print("Overall run time:")
            print(str((end - start) * 1000) + ' ms')

        except pymysql.MySQLError as e:
            print("Unable to connect using pymysql!")
            print(e, e.args)
        except OSError as err:
            print("OS error: " + format(err))
            cursor.close()
        except Exception as e:
            print("error message: {0}".format(e))
            cursor.close()

        return results

    def export_sql_results(self, sql_file, log_file, output_file,
                           output_file_delimited=False, delimiter=','):

        sql_in = open(sql_file, 'r')
        results_out = open(output_file, 'a')
        log_out = open(log_file, 'a')
        db_credentials = self.db_credentials
        db_group_suffix = self.db_host

        ''' Create ~/.my.cnf file '''
        config_out = open(os.environ['HOME'] + '/.my.cnf', 'w')
        config_out.write("[client" + db_group_suffix + "]" + "\n")
        config_out.write("user = " + db_credentials[db_group_suffix]['user'] + "\n")
        config_out.write("password = " + db_credentials[db_group_suffix]['password'] + "\n")
        config_out.write("host = " + db_credentials[db_group_suffix]['host'] + "\n")
        config_out.write("port = " + str(db_credentials[db_group_suffix]['port']) + "\n")
        config_out.close()

        ''' Set Permissions '''
        bash_command = "chmod 600 $HOME/.my.cnf"
        output, error = subprocess.Popen(bash_command, shell=True, stderr=subprocess.STDOUT,
                                         stdout=subprocess.PIPE).communicate()
        if sys.version_info.major == 3:
            output = output.splitlines()
            for line in output:
                log_out.write(str(line, 'utf-8'))
        else:
            log_out.write(output)

        try:
            start = time.time()
            print ("Started: " + sql_in.name + " at " + str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")
            log_out.write(
                "Started: " + sql_in.name + " at " + str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")

            mysql_string = """ mysql --defaults-group-suffix={db_group_suffix} --connect_timeout=7200 -A < {sql_file} > {output_file}
                            """.format(db_group_suffix=db_group_suffix, sql_file=os.path.abspath(sql_in.name),
                                       output_file=os.path.abspath(results_out.name))

            output, error = subprocess.Popen(mysql_string, shell=True, stderr=subprocess.STDOUT,
                                             stdout=subprocess.PIPE).communicate()

            if sys.version_info.major == 3:
                output = output.splitlines()
                for line in output:
                    log_out.write(str(line, 'utf-8'))
            else:
                log_out.write(output)

            end = time.time()
            print ("Ended: " + sql_in.name + " at " + str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")
            log_out.write("Ended: " + sql_in.name + " at " + str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")

            total_time = (end - start) * 1000
            print ("Total time taken to run: " + str(total_time) + " ms \n")
            log_out.write("Total time taken to run: " + str(total_time) + " ms \n")

            results_out.close()
            log_out.close()

            if output_file_delimited:
                subs_command = """sed -i -e 's/\t/""" + delimiter + """/g'  """ + os.path.abspath(results_out.name)
                subprocess.Popen(subs_command, shell=True, stderr=subprocess.STDOUT)

        except OSError as err:
            print("OS error: {0)".format(err))
            log_out.write("OS error: {0)".format(err))
        except Exception as e:
            print("error message: {0}".format(e))
            log_out.write("error message: {0}".format(e))

        # --------------------------------------------------------------------------------
        # Check the sql output for any errors
        error_position = 0
        if sys.version_info.major == 3:
            for line in output:
                out_line = str(line, 'utf-8')
                error_position = out_line.find('error')
                if error_position > 0:
                    break
        else:
            error_position = output.find('error', 0)

        if error_position > 0:
            print ("Error while processing " + sql_in.name)
            print ("**************************** START OF LOG *******************************")
            print(output)
            print ("***************************** END OF LOG ********************************")
            raise Exception('mysql execution failed - Error while running sql')

        return

    def load_csv_to_table(self, data_file, delimiter, schema_name, table_name):
        '''
        The function takes input file, file delimiter and a connection as input
        and loads the data into the table. File has to be in csv format and delimited.
        target table must be empty or will be truncated prior to load
        pymysql connection must be established prior to calling this function
        :param data_file:
        :param delimiter:
        :param conn:
        :param schema_name:
        :param table_name:
        :return:
        '''

        try:
            start = time.time()
            table = schema_name + '.' + table_name
            print("Started load: " + str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")

            cursor = self.conn.cursor()
            load_command = """
            LOAD DATA INFILE '{data_file}'
            INTO TABLE {table}
            FIELDS TERMINATED BY '{delimiter}'
            ENCLOSED BY '"'
            LINES TERMINATED BY '\r\n'
            IGNORE 1 LINES;
            """.format(data_file=data_file, table=table, delimiter=delimiter)
            cursor.execute(load_command)
            self.conn.commit()
            cursor.close()
            end = time.time()
            print("Ended load: " + str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")
            print("Overall run time:")
            print(str((end - start) * 1000) + ' ms')

        except pymysql.MySQLError as e:
            print("Unable to connect using pymysql!")
            print(e, e.args)
        except OSError as err:
            print("OS error: " + format(err))
            cursor.close()
        except Exception as e:
            print("error message: {0}".format(e))
            cursor.close()

        return
