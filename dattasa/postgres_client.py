import psycopg2
import sqlalchemy
from sqlalchemy import exc
import time
import datetime as dt
import sys, os
import subprocess
import shutil


class PostgresClient():
    '''
    Connects to greenplum database and executes sql queries against db
    '''

    def __init__(self, config_file, db_credentials, db_host):
        self.config_file = config_file
        self.db_credentials = db_credentials
        self.db_host = db_host
        return

    def get_db_conn(self, use_pyscopg2=True, verbose=False, connect_timeout=10):
        '''
        uses the credentials in config_yaml_file for db_host while initiating database connection
        connection can be established by either using pyscopg2 or sqlachemy modules
        :param use_pyscopg2:
        :param verbose:
        :return: database connection session that has been made using the credentials
        '''

        db_credentials = self.db_credentials
        db_host = self.db_host

        if use_pyscopg2:
            try:
                self.conn = psycopg2.connect(database=db_credentials[db_host]['database'],
                                             user=db_credentials[db_host]['user'],
                                             password=db_credentials[db_host]['password'],
                                             host=db_credentials[db_host]['host'],
                                             port=db_credentials[db_host]['port'],
                                             connect_timeout=connect_timeout)
                if verbose:
                    print("db credentials import succeeded. Connected using psycopg2 ..")

            except KeyError:
                if verbose:
                    print("No password found in yaml file. Using password from pgpass")
                self.conn = psycopg2.connect(database=db_credentials[db_host]['database'],
                                             user=db_credentials[db_host]['user'],
                                             host=db_credentials[db_host]['host'],
                                             port=db_credentials[db_host]['port'],
                                             connect_timeout=connect_timeout)
                if verbose:
                    print("db credentials import succeeded. Connected using psycopg2 ..")

            except psycopg2.Error as e:
                print("Unable to connect using psycopg2!")
                print(e.pgerror)
                print(e.diag.message_detail)
            except Exception as e:
                print(e, e.args)

        else:
            # Using sqlalchemy to make the connection
            try:
                connect_string = "postgresql://" + db_credentials[db_host]['user'] + ":" + \
                                 db_credentials[db_host]['password'] + \
                                 "@" + db_credentials[db_host]['host'] + "/" + \
                                 db_credentials[db_host]['database']
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
        :param verbose:
        :param connect_timeout:
        :return:
        '''

        db_credentials = self.db_credentials
        db_host = self.db_host

        try:
            self.conn = psycopg2.connect(database=db_credentials[db_host]['database'],
                                         user=db_credentials[db_host]['user'],
                                         password=db_credentials[db_host]['password'],
                                         host=db_credentials[db_host]['host'],
                                         port=db_credentials[db_host]['port'],
                                         connect_timeout=connect_timeout)
            if verbose:
                print("db credentials import succeeded. Connected using psycopg2 ..")

        except KeyError:
            if verbose:
                print("No password found in yaml file. Using password from pgpass")
            self.conn = psycopg2.connect(database=db_credentials[db_host]['database'],
                                         user=db_credentials[db_host]['user'],
                                         host=db_credentials[db_host]['host'],
                                         port=db_credentials[db_host]['port'],
                                         connect_timeout=connect_timeout)
            if verbose:
                print("db credentials import succeeded. Connected using psycopg2 ..")

        except psycopg2.Error as e:
            print("Unable to connect using psycopg2!")
            print(e.pgerror)
            print(e.diag.message_detail)
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

    def commit_query(self, verbose=False):
        try:
            self.conn.commit()
            if verbose:
                print("successful commit")
        except Exception as e:
            print("error message: {0}".format(e))

    def copy_query(self, query, file_path):
        try:
            f = open(file_path, 'r')
            self.cur.copy_expert(sql=query, file=f)
            self.conn.commit()
            print("copy insert successful")
        except Exception as e:
            print("copy insert error message: {0}".format(e))

    def get_results(self):
        return self.cur.fetchall()

    def rollback(self):
        self.conn.rollback()
        return None

    def close_connection(self):
        self.conn.close()
        return None

    def run_sql_file(self, sql_query_file):
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
            sql = s = " ".join(f.readlines())

            print (
            "Started executing: " + f.name + " at " + str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")
            cursor = self.conn.cursor()
            cursor.execute(sql)
            self.conn.commit()
            cursor.close()

            end = time.time()
            print (
            "Finished executing: " + f.name + " at " + str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")
            print ("Overall run time:")
            print (str((end - start) * 1000) + ' ms')

        except psycopg2.Error as e:
            print ("Unable to connect!")
            print (e.pgerror)
            print (e.diag.message_detail)
        except OSError as err:
            print("OS error: " + format(err))
            cursor.close()
        except Exception as e:
            print("error message: {0}".format(e))
            cursor.close()

        return

    def run_sql_command(self, sql_command, verbose=False, fetch=False):
        '''
        The function takes a sql_coomand and a connection as input and will run the SQL query using given connection
        pyscopg2 connection must be established prior to calling this function.
        fecth=True for a query that returns rows at end of the sql
        :param sql_command:
        :param connection:
        :param fetch:
        :return: results of sql query
        '''

        try:
            start = time.time()
            if verbose:
                print ("Started executing: " + " at " + str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")

            cursor = self.conn.cursor()
            if fetch:
                cursor.execute(sql_command)
                results = cursor.fetchall()
            else:
                cursor.execute(sql_command)
                self.conn.commit()
                results = None

            cursor.close()
            end = time.time()
            if verbose:
                print ("Finished executing: " + " at " + str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")
                print ("Overall run time:")
                print (str((end - start) * 1000) + ' ms')

        except psycopg2.Error as e:
            print ("Unable to connect!")
            print (e.pgerror)
            print (e.diag.message_detail)
        except OSError as err:
            print("OS error: " + format(err))
            cursor.close()
        except Exception as e:
            print("error message: {0}".format(e))
            cursor.close()

        return results

    def export_sql_results(self, sql_file, log_file, output_file,
                           output_file_delimited=False, delimiter=','):
        '''
        The function takes a file as input and will run the SQL query using given connection
        pyscopg2 connection must be established prior to calling this function.
        results are redirected to output_file that can be delimited
        :param sql_file:
        :param output_file:
        :param output_file_delimited:
        :param delimiter:
        :return: None
        '''

        try:
            log_out = open(log_file, 'a')

            f = open(sql_file, 'r')
            start = time.time()
            sql = s = " ".join(f.readlines())

            start = time.time()
            print (
            "Started executing: " + f.name + " at " + str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")
            log_out.write(
                "Started executing: " + f.name + " at " + str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")
            cursor = self.conn.cursor()

            if output_file_delimited:
                COPY_STATEMENT = """
                COPY ({sql}) TO STDOUT
                    WITH CSV HEADER
                    DELIMITER AS '{delimiter}'
                """.format(sql=sql, delimiter=delimiter)
            else:
                COPY_STATEMENT = """
                COPY ({sql}) TO STDOUT
                WITH CSV HEADER
                """.format(sql=sql)
            with open(output_file, 'w') as out:
                cursor.copy_expert(COPY_STATEMENT, out)
            cursor.close()

            end = time.time()
            print ("Ended executing: " + f.name + " at " + str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")
            print ("Overall run time:")
            print (str((end - start) * 1000) + ' ms')
            log_out.write(
                "Ended executing: " + f.name + " at " + str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")
            log_out.write("Overall run time:")
            log_out.write(str((end - start) * 1000) + ' ms')

        except psycopg2.Error as e:
            print ("Unable to export!")
            print (e.pgerror)
            print (e.diag.message_detail)
            cursor.close()
        except OSError as err:
            print("OS error: " + format(err))
            cursor.close()
        except Exception as e:
            print("error message: {0}".format(e))
            cursor.close()

        return

    def load_csv_to_table(self, data_file, delimiter, schema_name, table_name, clear_target=False):
        '''
        The function takes input file, file delimiter and a connection as input
        and loads the data into the table. File has to be in csv format and delimited.
        target table must be empty or will be truncated prior to load
        pyscopg2 connection must be established prior to calling this function
        :param data_file:
        :param delimiter:
        :param conn:
        :param schema_name:
        :param table_name:
        :return:
        '''

        try:
            f = open(data_file, 'r')
            cur = self.conn.cursor()
            objectName = schema_name + "." + table_name
            if clear_target:
                query = "truncate " + objectName + ";"
                cur.execute(query)
            SQL_STATEMENT = """
            COPY %s FROM STDIN WITH
                CSV
                HEADER
                DELIMITER AS '{delimiter}'
            """.format(delimiter=delimiter)
            cur.copy_expert(sql=SQL_STATEMENT % objectName, file=f)
            self.conn.commit()
            cur.close()

        except psycopg2.Error as e:
            print ("Unable to load!")
            print (e.pgerror)
            print (e.diag.message_detail)
        except OSError as err:
            print("OS error: " + format(err))
            cur.close()
        except Exception as e:
            print("error message: {0}".format(e))
            cur.close()

        return


class GreenplumClient(PostgresClient):
    def __init__(self, config_file, db_credentials, db_host):
        self.config_file = config_file
        self.db_credentials = db_credentials
        self.db_host = db_host
        return

    def run_psql_file(self, sql_file, log_file, output_file,
                      output_file_delimited=False, delimiter=','):
        '''
        The function takes a sql_file, log file, output_file and format of output_file as input
        and will run the sql queries in the sql file using psql
        psql uses the credentials in config_yaml_file for db_host while initiating database connection
        This module will raise an exception if any ERROR is encountered while running the sql file
        :param sql_file:
        :param log_file:
        :param output_file:
        :param output_file_delimited:
        :return:
        '''

        db_credentials = self.db_credentials
        db_host = self.db_host

        sql_in = open(sql_file, 'r')
        results_out = open(output_file, 'a')
        log_out = open(log_file, 'a')

        try:

            start = time.time()
            print ("Started: " + sql_in.name + " at " + str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")
            log_out.write(
                "Started: " + sql_in.name + " at " + str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")

            if output_file_delimited:
                psql_string = " psql -h " + db_credentials[db_host]['host'] + " -U " + db_credentials[db_host]['user'] + \
                              " -d " + db_credentials[db_host]['database'] + " -p " + str(
                    db_credentials[db_host]['port']) + \
                              " -f " + os.path.abspath(sql_in.name) + " -A -F '" + delimiter + "' -X  -t -q " + \
                              " > " + os.path.abspath(results_out.name)

            else:
                psql_string = " psql -h " + db_credentials[db_host]['host'] + " -U " + db_credentials[db_host]['user'] + \
                              " -d " + db_credentials[db_host]['database'] + " -p " + str(
                    db_credentials[db_host]['port']) + \
                              " -f " + os.path.abspath(sql_in.name) + " -t > " + os.path.abspath(results_out.name)

            output, error = subprocess.Popen(psql_string, shell=True, stderr=subprocess.STDOUT,
                                              stdout=subprocess.PIPE).communicate()
            if sys.version_info.major == 3:
                output = output.splitlines()
                for line in output:
                    log_out.write(str(line, 'utf-8'))
            else:
                log_out.write(output)

            end = time.time()
            print ("Ended: " + sql_in.name + " at " + str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")
            log_out.write(
                "Ended: " + sql_in.name + " at " + str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")

            total_time = (end - start) * 1000
            print ("Total time taken to run: " + str(total_time) + " ms \n")
            log_out.write("Total time taken to run: " + str(total_time) + " ms \n")

            results_out.close()
            log_out.close()

        except OSError as err:
            print("OS error: {0)".format(err))
            log_out.write("OS error: {0)".format(err))
        except Exception as e:
            print("error message: {0}".format(e))
            log_out.write("error message: {0}".format(e))

        # --------------------------------------------------------------------------------
        # Check the log file for any errors
        log_in = open(log_file, 'r')
        line_no = 0
        for log_line in log_in:
            line_no += 1
            error_position = log_line.find("ERROR:")
            if error_position > 0:
                print ("Error while processing " + sql_in.name)
                print ("Check line number " + str(line_no) + " in " + log_in.name)
                raise Exception('psql execution failed - Error while running sql')
                break
        log_in.close()

        return

    def run_psql_command(self, sql_command, log_file, output_file,
                         output_file_delimited=False, delimiter=','):
        '''
        The function takes a sql_command, log file, output_file and format of output_file as input
        and will run the sql command using psql
        psql uses the credentials in config_yaml_file for db_host while initiating database connection
        This module will raise an exception if any ERROR is encountered while running the sql file
        :param sql_command:
        :param log_file:
        :param output_file:
        :param output_file_delimited:
        :return:
        '''

        db_credentials = self.db_credentials
        db_host = self.db_host

        results_out = open(output_file, 'a')
        log_out = open(log_file, 'a')

        try:
            start = time.time()
            log_out.write("Started: " + " at " + str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")

            if output_file_delimited:
                psql_string = " psql -h " + db_credentials[db_host]['host'] + " -U " + db_credentials[db_host]['user'] + \
                              " -d " + db_credentials[db_host]['database'] + " -p " + str(
                    db_credentials[db_host]['port']) + \
                              """ -c " """ + sql_command + """ " -A -F '""" + delimiter + """' -X  -t -q """ + \
                              " > " + os.path.abspath(results_out.name)

            else:
                psql_string = " psql -h " + db_credentials[db_host]['host'] + " -U " + db_credentials[db_host]['user'] + \
                              " -d " + db_credentials[db_host]['database'] + " -p " + str(
                    db_credentials[db_host]['port']) + \
                              """ -c " """ + sql_command + """ " """ + " > " + os.path.abspath(results_out.name)

            output, error = subprocess.Popen(psql_string, shell=True, stderr=subprocess.STDOUT,
                                              stdout=subprocess.PIPE).communicate()
            if sys.version_info.major == 3:
                output = output.splitlines()
                for line in output:
                    log_out.write(str(line, 'utf-8'))
            else:
                log_out.write(output)

            end = time.time()
            log_out.write("Ended: " + " at " + str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")

            total_time = (end - start) * 1000
            log_out.write("Total time taken to run: " + str(total_time) + " ms \n")

            results_out.close()
            log_out.close()

        except OSError as err:
            print("OS error: {0}".format(err))
            log_out.write("OS error: {0}".format(err))
        except Exception as e:
            print("error message: {0}".format(e))
            log_out.write("error message: {0}".format(e))

        # --------------------------------------------------------------------------------
        # Check the out file for any errors
        results_in = open(output_file, 'r')
        line_no = 0
        for result_line in results_in:
            line_no += 1
            error_position = result_line.find("ERROR:")
            if error_position > 0:
                print ("Error while processing ")
                print ("Check line number " + str(line_no) + " in " + results_in.name)
                results_in.close()
                raise Exception('psql execution failed - Error while running sql')
                break
        results_in.close()

        return

    def run_gpload_script(self, data_file, gpload_script_location, log_file,
                          gp_target_table, gp_error_table, file_delimiter='|',
                          null_string='', full_refresh=True, clear_target=True,
                          gpload_file_name='',
                          header=True, column_list=[], error_limit="2"):
        '''

        Reference -
        https://gpdb.docs.pivotal.io/510/admin_guide/load/topics/g-loading-data-with-gpload.html

        :param data_file: 
        :param gpload_script_location: 
        :param log_file: 
        :param gp_target_table: 
        :param gp_error_table: 
        :param file_delimiter: 
        :param null_string: 
        :param full_refresh: 
        :param clear_target:
        :param gpload_file_name:
        :param header: 
        :param column_list:
        :param error_limit: 
        :return: 
        '''

        db_credentials = self.db_credentials
        db_host = self.db_host

        if gpload_file_name == '':
            gpload_srcipt_file_name = gpload_script_location + "/gpload_" + gp_target_table + ".yml"
        else:
            gpload_srcipt_file_name = gpload_script_location + "/gpload_" + gpload_file_name + ".yml"

        gpload_srcipt_file = open(gpload_srcipt_file_name, 'w')
        log_out = open(log_file, 'a')

        try:
            start = time.time()
            log_out.write("Started: " + " at " + str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")

            gpload_srcipt_file.write("VERSION: 1.0.0.1\n")
            gpload_srcipt_file.write("DATABASE: {0}\n".format(db_credentials[db_host]['database']))
            gpload_srcipt_file.write("USER: {0}\n".format(db_credentials[db_host]['user']))
            gpload_srcipt_file.write("HOST: {0}\n".format(db_credentials[db_host]['host']))
            gpload_srcipt_file.write("PORT: {0}\n".format(db_credentials[db_host]['port']))
            gpload_srcipt_file.write("GPLOAD: \n")
            gpload_srcipt_file.write("   INPUT: \n")
            gpload_srcipt_file.write("      - SOURCE: \n")
            gpload_srcipt_file.write("         FILE: \n")

            if isinstance(data_file, list):
                for file_name in data_file:
                    gpload_srcipt_file.write("             - {0}\n".format(file_name))
            else:
                gpload_srcipt_file.write("             - {0}\n".format(data_file))

            if len(column_list) > 0:
                gpload_srcipt_file.write("      - COLUMNS:\n")
                for column_name in column_list:
                    gpload_srcipt_file.write("              - {0}\n".format(column_name))

            gpload_srcipt_file.write("      - FORMAT: text\n")
            if header:
                gpload_srcipt_file.write("      - HEADER: true\n")
            else:
                gpload_srcipt_file.write("      - HEADER: false\n")
            gpload_srcipt_file.write("      - DELIMITER: '{0}'\n".format(file_delimiter))
            gpload_srcipt_file.write("      - NULL_AS: '{0}'\n".format(null_string))
            gpload_srcipt_file.write("""      - ESCAPE: "OFF"\n""")
            gpload_srcipt_file.write("      - ERROR_LIMIT: {0}\n".format(error_limit))
            gpload_srcipt_file.write("      - ERROR_TABLE: {0}\n".format(gp_error_table))

            gpload_srcipt_file.write("   OUTPUT: \n")
            gpload_srcipt_file.write("       - TABLE: {0}\n".format(gp_target_table))
            gpload_srcipt_file.write("       - MODE: INSERT\n")

            if full_refresh or clear_target:
                gpload_srcipt_file.write("   SQL: \n")

            if full_refresh:
                gpload_srcipt_file.write("""      - BEFORE: "TRUNCATE {0};"\n""".format(gp_target_table))

            if clear_target:
                gpload_srcipt_file.write(
                    """      - AFTER: "ANALYZE {0}; ALTER TABLE {1} SET WITH (reorganize=true)"\n""".
                    format(gp_target_table, gp_target_table))

            gpload_srcipt_file.close()

            gpload_bash_file_name = gpload_script_location + "/gpload_" + gp_target_table + ".sh"
            gpload_bash_file = open(gpload_bash_file_name, 'w')
            gpload_bash_file.write("source " + os.environ['GPLOAD_HOME'] + "/" + "greenplum_loaders_path.sh" + "\n")
            gpload_bash_file.write("gpload -f " + gpload_srcipt_file_name + "\n")
            gpload_bash_file.close()
            gpload_command = "chmod 755 " + gpload_bash_file_name + " && bash " + gpload_bash_file_name
            output, error = subprocess.Popen(gpload_command, shell=True, stderr=subprocess.STDOUT,
                                              stdout=subprocess.PIPE).communicate()

            if sys.version_info.major == 3:
                output = output.splitlines()
                for line in output:
                    log_out.write(str(line, 'utf-8'))
                    print(str(line, 'utf-8'))
            else:
                log_out.write(output)
                print(output)

            end = time.time()
            log_out.write("Ended: " + " at " + str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")

            total_time = (end - start) * 1000
            log_out.write("Total time taken to run: " + str(total_time) + " ms \n")
            log_out.close()

        except OSError as err:
            print("OS error: {0}".format(err))
            log_out.write("OS error: {0}".format(err))
        except Exception as e:
            print("error message: {0}".format(e))
            log_out.write("error message: {0}".format(e))

        # --------------------------------------------------------------------------------
        # Check the log file for any errors
        log_in = open(log_file, 'r')
        line_no = 0
        for log_line in log_in:
            line_no += 1
            error_position = log_line.find("ERROR:")
            if error_position > 0:
                print("Error while processing ")
                print("Check line number " + str(line_no) + " in " + log_in.name)
                log_in.close()
                raise Exception('gpload execution failed - Error while running gpload')
                break
        log_in.close()

        return

    def __remove_line_feeds(self, file_name):

        '''
        
        :param file_name: 
        :return: 
        
        head -n 1 filename | od -c  to find out whats wrong
        
        tr -d '\n' <filename
        tr -d '\r\n' <filename

        '''

        temp_out_file_name = file_name + ".tmp"

        repl_command = """tr -d '\n' < {0}  > {1}""".format(file_name, temp_out_file_name)

        output, error = subprocess.Popen(repl_command, shell=True, stderr=subprocess.STDOUT,
                                         stdout=subprocess.PIPE).communicate()

        if sys.version_info.major == 3:
            output = output.splitlines()
            for line in output:
                print(str(line, 'utf-8'))
        else:
            print(output)

        shutil.move(temp_out_file_name, file_name)
