from simple_salesforce import Salesforce
import sys


class SalesforceClient():
    def __init__(self, api_credentials):
        self.api_credentials = api_credentials

        try:
            sfdc_client = Salesforce(username=api_credentials['salesforce_user'],
                                     password=api_credentials['salesforce_password'],
                                     security_token=api_credentials['apisecret'])
            self.sfdc_client = sfdc_client

        except Exception as e:
            print(e, e.args)
            raise Exception("Unable to connect to salesforce")

        return

    def request_data(self, object_name, field_list=[], query_condition='', query_all=False, debug=False):

        if len(field_list) == 0:
            raise Exception("field_list is empty. cannot export results to csv")
        else:
            output_fields = ','.join(field_list)

        sfdc_query = "SELECT " + output_fields + " FROM " + object_name
        if query_condition != '':
            sfdc_query += " WHERE " + query_condition

        if debug:
            print("Fetching results for query - \n")
            print(sfdc_query)

        query_result = self.sfdc_client.query(sfdc_query)
        record_list = query_result['records']  # dictionary of results!
        json_response = []
        while len(record_list) > 0:
            for record in record_list:
                message = {}
                for field_lkp in field_list:
                    field_name = field_lkp.split('.')
                    first_name = field_name[0]
                    try:
                        second_name = field_name[1]
                    except IndexError:
                        second_name = ''

                    if record[first_name] is None:
                        attr_value = ""
                    else:
                        if second_name != '':
                            attr_value = record[first_name][second_name]
                        else:
                            attr_value = record[first_name]

                    if sys.version_info >= (3, 0, 0):
                        # for Python 3
                        if isinstance(attr_value, bytes):
                            attr_value = attr_value.decode('ascii')  # or  s = str(s)[2:-1]
                    else:
                        # for Python 2
                        if type(attr_value) == unicode:
                            attr_value = attr_value.encode('utf-8')

                    message[field_lkp] = attr_value
                json_response.append(message)

            try:
                url = query_result['nextRecordsUrl']
                if debug:
                    print(url)

                if query_all:
                    query_result = self.sfdc_client.query_more(url, True)
                    record_list = query_result['records']  # dictionary of results!
                else:
                    print (
                        "WARNING - Resultset exceeds 2000 rows. Stopping at 2000 rows. Use query_all=True to get all rows")
                    record_list = []
            except KeyError:
                record_list = []

        return json_response

    def export_query_results_to_csv(self, object_name, output_file_name, field_list=[],
                                    query_condition='', file_delimiter=',', debug=False):

        if len(field_list) == 0:
            raise Exception("field_list is empty. cannot export results to csv")
        else:
            out_field_list = []
            for attr_value in field_list:
                out_field_list.append(attr_value['field'])
            output_fields = ','.join(out_field_list)

        sfdc_query = "SELECT " + output_fields + " FROM " + object_name
        if query_condition != '':
            sfdc_query += " WHERE " + query_condition

        if debug:
            print("Fetching results for query - \n")
            print(sfdc_query)

        try:
            query_result = self.sfdc_client.query(sfdc_query)
            record_list = query_result['records']  # dictionary of results!
        except Exception as e:
            print(e, e.args)
            raise Exception("Unable to fetch results from salesforce")

        sfdc_file = open(output_file_name , 'w')
        try:
            for record in record_list:

                row = ''
                for field_lkp in field_list:
                    field_name = field_lkp['field']
                    field_name = field_name.split('.')
                    first_name = field_name[0]
                    try:
                        second_name = field_name[1]
                    except IndexError:
                        second_name = ''

                    if record[first_name] is None:
                        row += ''
                    else:
                        if second_name != '':
                            attr_value = record[first_name][second_name]
                        else:
                            attr_value = record[first_name]

                        ''' remove non-ascii characters by ignoring errors '''
                        if sys.version_info.major == 2:
                            if type(record[first_name]) == unicode:
                                attr_value = str(attr_value.encode("utf-8", errors="ignore"))
                            else:
                                attr_value = str(attr_value)
                        else:
                            if type(record[first_name]) == unicode:
                                attr_value = str(attr_value.decode("ascii", errors="ignore"))
                            else:
                                attr_value = str(attr_value)

                        try:
                            field_is_string = field_lkp['is_string']
                            if field_is_string == 0:
                                string_field = False
                            else:
                                raise Exception("Incorrect setting for is_string. Value should be number 0")
                        except KeyError:
                            string_field = True

                        if string_field:
                            row += '\"' + attr_value + '\"'
                        else:
                            row += attr_value

                    ''' Add delimiter if not the last field '''
                    if field_lkp['field'] != field_list[-1]['field']:
                        row += file_delimiter

                row += '\n'
                sfdc_file.write(row)

        except KeyError as e:
            print(e, e.args)
            raise Exception("Wrong parameter passed. Check values in attribute_list")

        sfdc_file.close()

        return

