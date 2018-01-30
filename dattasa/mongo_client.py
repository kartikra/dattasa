import pymongo
import os, subprocess
import sys
import re


class MongoClient():
    '''
    Connects to mongo database and executes scripts
    '''
    def __init__(self, config_file, db_credentials, db_host, debug=False):

        self.config_file = config_file
        self.db_credentials = db_credentials
        self.db_host = db_host
        self.debug = debug

        return

    def get_db_conn(self, auth_db="admin"):

        mongo_credentials = self.db_credentials[self.db_host]
        mongo_host = mongo_credentials['host']
        mongo_port = mongo_credentials['port']
        mongo_user = mongo_credentials['user']
        mongo_password = mongo_credentials['password']

        try:
            mongo_client = pymongo.MongoClient(str(mongo_host), int(mongo_port))
        except pymongo.errors.ConnectionFailure as e:
            print ("Could not connect to server: {0}".format(e))
            raise Exception('Unable to connect to mongo host ' + mongo_host)

        if auth_db != "admin":
            try:
                mongo_auth_database = mongo_credentials['auth_database']
            except:
                mongo_auth_database = "admin"
        else:
            mongo_auth_database = auth_db

        try:
            conn = mongo_client[mongo_auth_database]
            conn.authenticate(mongo_user, mongo_password)
            self.mongo_client = mongo_client
        except pymongo.errors.ConnectionFailure as e:
            print ("authentication on auth_database {0} failed".format(mongo_auth_database))
            print ("Could not connect to server: {0}".format(e))
            mongo_client.close()
            raise Exception('Unable to authenticate using auth_database ' + mongo_auth_database)

        return self.mongo_client

    def close_connection(self):
        self.mongo_client.close()
        return

    def drop_collection(self, collection_name, database_name=""):

        mongo_credentials = self.db_credentials[self.db_host]
        if database_name == "":
            mongo_database = mongo_credentials['database']
        else:
            mongo_database = database_name
        result = self.mongo_client[mongo_database].drop_collection(collection_name)
        return result

    def set_mongo_collection(self, collection_name, database_name=""):

        mongo_credentials = self.db_credentials[self.db_host]
        if database_name == "":
            mongo_database = mongo_credentials['database']
        else:
            mongo_database = database_name
        self.collection = self.mongo_client[mongo_database][collection_name]
        return

    def create_collection_index(self, index_name, field_list=[], unique=False,
                                drop_index=True, default_language='english'):

        mongo_collection = self.collection
        index_field_list = []
        for field in field_list:
            for key, value in field.items():
                if value == "A":
                    field_tuple = (key, pymongo.ASCENDING)
                elif value == "D":
                    field_tuple = (key, pymongo.DESCENDING)
                index_field_list.append(field_tuple)

        if len(index_field_list) > 0:
            if drop_index:
                try:
                    mongo_collection.drop_index(index_name)
                except:
                    pass

            result = mongo_collection.create_index(index_field_list,
                                          name=index_name,
                                          unique=unique,
                                          default_language=default_language)
        return result

    def drop_collection_index(self, index_name):
        result = self.collection.drop_index(index_name)
        return result

    def reindex_collection(self):
        result = self.collection.reindex()
        return result

    def run_mongo_script_file(self, script_file, log_file, out_file='',
                              indicator_text="my results start from here"):

        mongo_credentials = self.db_credentials[self.db_host]
        mongo_user = mongo_credentials['user']
        mongo_password = mongo_credentials['password']
        mongo_host = mongo_credentials['host']
        mongo_port = mongo_credentials['port']
        mongo_database = mongo_credentials['database']
        mongo_auth_database = mongo_credentials['auth_database']

        ''' validate authentication '''
        mongo_client = self.get_db_conn(auth_db=mongo_auth_database)
        mongo_client.close()

        mongo_script_string = "mongo --host " + str(mongo_host) + \
                              " --port " + str(mongo_port) + \
                              " --authenticationDatabase " + str(mongo_auth_database) + \
                              " -u " + str(mongo_user) + \
                              " -p " + str(mongo_password) + \
                              " --quiet " + mongo_database + \
                              " < " + os.path.abspath(script_file)

        if out_file != '':
            mongo_script_string += " > " + os.path.abspath(out_file)

        if self.debug:
            print(mongo_script_string)

        output, error = subprocess.Popen(mongo_script_string, shell=True, stderr=subprocess.STDOUT,
                                         stdout=subprocess.PIPE).communicate()
        log_out = open(log_file, 'a')
        if sys.version_info.major == 3:
            output = output.splitlines()
            for line in output:
                log_out.write(str(line, 'utf-8'))
                print(str(line, 'utf-8'))
        else:
            log_out.write(output)
            print(output)

        if out_file != '':
            # Check the out file for any errors
            log_in = open(out_file, 'r')
            line_no = 0
            for log_line in log_in:
                line_no += 1
                error_position = log_line.find("errmsg")
                if error_position > 0:
                    print("Error while processing ")
                    print("Check line number " + str(line_no) + " in " + log_in.name)
                    log_in.close()
                    raise Exception('mongo script execution failed')
                    break
            log_in.close()

        # Check the log file for any errors
        log_in = open(log_file, 'r')
        line_no = 0
        for log_line in log_in:
            line_no += 1
            error_position = log_line.find("errmsg")
            if error_position > 0:
                print("Error while processing ")
                print("Check line number " + str(line_no) + " in " + log_in.name)
                log_in.close()
                raise Exception('mongo script execution failed')
                break
        log_in.close()

        # Generate final results file
        format_output = """ line_number=`cat {out_file} | grep -n "{text}" | cut -f1 -d':'` &&
        line_number=`expr $line_number + 1` && tail -n +$line_number {out_file} > $HOME/temp1.txt &&
        mv $HOME/temp1.txt {out_file}
        """.format(out_file=os.path.abspath(out_file), text=indicator_text)

        if self.debug:
            print(format_output)
        out, err = subprocess.Popen(format_output, shell=True, stderr=subprocess.STDOUT,
                                    stdout=subprocess.PIPE).communicate()
        if sys.version_info.major == 3:
            out = out.splitlines()
            for line in out:
                print(str(line, 'utf-8'))
        else:
            print(out)

        return

    def load_file_to_collection(self, collection_name, input_file, log_file,
                                database_name="", csv_file=False, drop_target=False):

        mongo_credentials = self.db_credentials[self.db_host]
        mongo_user = mongo_credentials['user']
        mongo_password = mongo_credentials['password']
        mongo_host = mongo_credentials['host']
        mongo_port = mongo_credentials['port']
        if database_name == "":
            mongo_database = mongo_credentials['database']
        else:
            mongo_database = database_name
        mongo_auth_database = mongo_credentials['auth_database']

        ''' validate authentication '''
        mongo_client = self.get_db_conn(auth_db=mongo_auth_database)
        mongo_client.close()

        mongo_import_string = "mongoimport --host " + str(mongo_host) + \
                              " --port " + str(mongo_port) + \
                              " --authenticationDatabase " + str(mongo_auth_database) + \
                              " -u " + str(mongo_user) + \
                              " -p " + str(mongo_password) + " -d " + str(mongo_database) + \
                              " -c " + collection_name

        if csv_file:
            mongo_import_string += " --type csv --headerline "

        mongo_import_string += " --file " + os.path.abspath(input_file)

        if drop_target:
            mongo_import_string += " --drop "

        if self.debug:
            print(mongo_import_string)

        output, error = subprocess.Popen(mongo_import_string, shell=True, stderr=subprocess.STDOUT,
                                         stdout=subprocess.PIPE).communicate()
        log_out = open(log_file, 'a')
        if sys.version_info.major == 3:
            output = output.splitlines()
            for line in output:
                log_out.write(str(line, 'utf-8'))
                print(str(line, 'utf-8'))
        else:
            log_out.write(output)
            print(output)

        # Check the log file for any errors
        log_in = open(log_file, 'r')
        line_no = 0
        for log_line in log_in:
            line_no += 1
            error_position = log_line.find("Failed")
            if error_position > 0:
                print("Error while processing ")
                print("Check line number " + str(line_no) + " in " + log_in.name)
                log_in.close()
                raise Exception('import to mongodb failed - Error while running mongoimport')
                break
        log_in.close()

        return

    def export_collection_to_file(self, collection_name, output_file, log_file,
                                  database_name="", csv_file=False, csv_fields=""):

        mongo_credentials = self.db_credentials[self.db_host]
        mongo_user = mongo_credentials['user']
        mongo_password = mongo_credentials['password']
        mongo_host = mongo_credentials['host']
        mongo_port = mongo_credentials['port']
        if database_name == "":
            mongo_database = mongo_credentials['database']
        else:
            mongo_database = database_name
        mongo_auth_database = mongo_credentials['auth_database']

        ''' validate authentication '''
        mongo_client = self.get_db_conn(auth_db=mongo_auth_database)
        mongo_client.close()

        mongo_export_string = "mongoexport --host " + str(mongo_host) + \
                              " --port " + str(mongo_port) + \
                              " --authenticationDatabase " + str(mongo_auth_database) + \
                              " -u " + str(mongo_user) + \
                              " -p " + str(mongo_password) + " -d " + str(mongo_database) + \
                              " -c " + collection_name

        if csv_file:
            mongo_export_string += " --type=csv --fields " + csv_fields

        mongo_export_string += " --out " + os.path.abspath(output_file)

        if self.debug:
            print(mongo_export_string)

        output, error = subprocess.Popen(mongo_export_string, shell=True, stderr=subprocess.STDOUT,
                                         stdout=subprocess.PIPE).communicate()
        log_out = open(log_file, 'a')
        if sys.version_info.major == 3:
            output = output.splitlines()
            for line in output:
                log_out.write(str(line, 'utf-8'))
                print(str(line, 'utf-8'))
        else:
            log_out.write(output)
            print(output)

        # --------------------------------------------------------------------------------
        # Check the log file for any errors
        log_in = open(log_file, 'r')
        line_no = 0
        for log_line in log_in:
            line_no += 1
            error_position = log_line.find("Failed")
            if error_position > 0:
                print("Error while processing ")
                print("Check line number " + str(line_no) + " in " + log_in.name)
                log_in.close()
                raise Exception('export from mongodb failed - Error while running mongoexport')
                break
        log_in.close()

        return

    def export_mixpanel_query_results(self, collection_name, mixpanel_query, mixpanel_fields,
                                      results_csv_file, constant_fields={}, field_mapping={},
                                      file_delimiter=",", header_row=False):

        mongo_credentials = self.db_credentials[self.db_host]
        mongo_database = mongo_credentials['database']
        mongo_auth_database = mongo_credentials['auth_database']

        ''' validate authentication '''
        mongo_client = self.get_db_conn(auth_db=mongo_auth_database)

        ''' initialize collection '''
        mongo_collection = mongo_client[mongo_database][collection_name]

        with open(results_csv_file, 'w') as f:

            ''' header row '''
            if header_row:
                out_header_row = ""
                for config_data in field_mapping:
                    gp_column_name = config_data['gp_column_name']
                    out_header_row += str(gp_column_name) + str(file_delimiter)

                f.write(out_header_row.rstrip(str(file_delimiter)) + '/n')

            ''' detail row - process each row in the order in which the fields appear '''
            cursor = mongo_collection.find(mixpanel_query, mixpanel_fields)
            key_list = mixpanel_fields.keys()

            for row in cursor:

                out_dict = dict()
                out_dict["id"] = row["_id"]
                for key in key_list:
                    if key.startswith("properties"):
                        property_name = key.split('.')[1]
                        try:
                            field_value = row["properties"][property_name]
                        except KeyError:
                            field_value = ""
                    else:
                        field_value = row[key]

                    try:
                        out_dict[str(key)] = field_value.replace(file_delimiter, "").replace('\r\n', '').replace('\r', '').replace('\n', '')
                    except AttributeError:
                        out_dict[str(key)] = field_value

                out_row = ""
                for config_data in field_mapping:

                    gp_column_name = config_data['gp_column_name']
                    gp_column_position = config_data['gp_column_position']
                    gp_column_type = config_data['gp_column_type']
                    json_field_name = config_data['json_field']

                    if config_data['gp_value_is_constant']:
                        try:
                            column_value = constant_fields[gp_column_name]
                        except KeyError:
                            column_value = ""
                    else:
                        try:
                            column_value = out_dict[json_field_name]
                        except KeyError:
                            column_value = ""

                    if gp_column_type == "varchar":
                        try:
                            column_value = str(column_value)
                        except UnicodeError:
                            column_value = re.sub(r'[^\x00-\x7f]', r'', column_value)
                    else:
                        column_value = str(column_value)

                    if column_value == "None":
                        column_value = ""

                    if gp_column_position == 1:
                        out_row += column_value
                    else:
                        out_row += file_delimiter + column_value

                f.write(str(out_row) + "\n")

            f.close
            mongo_client.close()

        return

