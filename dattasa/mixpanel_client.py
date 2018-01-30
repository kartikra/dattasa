import base64
import json
import urllib
import sys
import subprocess
import requests

maj_ver = sys.version_info.major
if maj_ver == 2:
    import urllib2


class MixpanelClient():

    API_ENDPOINT = 'https://mixpanel.com/api'
    EXPORT_ENDPOINT = 'https://data.mixpanel.com/api'
    VERSION = '2.0'

    def __init__(self, api_credentials):
        self.api_credentials = api_credentials

    def request_data(self, methods, params={}, output_file_name='',
                     secure=False, debug=False, http_method="GET", timeout=1200):
        '''
        
        :param methods: List of methods to be joined, e.g. ['events', 'properties', 'values']
                        will give us https://mixpanel.com/api/2.0/events/properties/values/
                        
                        if method is export then 
                        https://data.mixpanel.com/api/2.0/export params
        :param params:  Extra parameters associated with method
        :param output_file_name: 
        :param secure: secure - True: verifies ssl certificate else check is bypassed
        :param debug: debug - False: don't print interim results
        :param http_method: GET by default
        :param timeout: 1200 by default
        :return: 
        '''

        mixpanel_credentials = self.api_credentials
        api_secret = mixpanel_credentials['apisecret']

        if methods[0] == 'export' and len(methods) == 1:
            ''' export api request '''
            request_url = '/'.join([self.EXPORT_ENDPOINT, str(self.VERSION)] + methods)
        else:
            request_url = '/'.join([self.API_ENDPOINT, str(self.VERSION)] + methods)

        if http_method == 'GET':
            data = None
            request_url = request_url + '/?' + self.unicode_urlencode(params)
            if debug:
                print (request_url)
        else:
            data = self.unicode_urlencode(params)

        if maj_ver == 2:
            headers = {'Authorization': 'Basic {encoded_secret}'.format(encoded_secret=base64.b64encode(api_secret))}

        if maj_ver == 3:
            headers = {'Authorization': 'Basic {encoded_secret}'.format(encoded_secret=api_secret)}

        resp = requests.get(request_url, verify=secure, headers=headers, timeout=timeout)

        if debug:
            print ("text : {0}".format(resp.text.encode('utf-8')))
            print ("reason : {0}".format(resp.reason.encode('utf-8')))

        if resp.status_code == 200:
            if "export terminated early" in resp.text.encode('utf-8'):
                print ("mixpanel API returned warning - {0}".format(resp.text))
                print ("try again later or contact mixpanel support at support@mixpanel.com")
                raise Warning("unable to export events from mixpanel")
            else:
                if output_file_name == '':
                    print ("No output file name found in batch mode")
                else:
                    resp_data = resp.text.split('\n')
                    with open(output_file_name, 'w') as out:
                        for row in resp_data:
                            out.write(row.encode('utf-8') + '\n')
                        out.close
        else:
            print("Response from API - {0}".format(resp.text))
            print("Error Reason - {0}".format(resp.reason))
            print("HTTP Status Code {0}".format(resp.status_code))
            raise Exception("Mixpanel API Request failed")

    def stream_data(self, methods, params={}, secure=False, debug=False,
                    http_method="GET", timeout=1200):
        '''
        :param methods: List of methods to be joined, e.g. ['events', 'properties', 'values']
                        will give us https://mixpanel.com/api/2.0/events/properties/values/

                        if method is export then 
                        https://data.mixpanel.com/api/2.0/export params
        :param params:  Extra parameters associated with method
        :param secure: secure - True: verifies ssl certificate else check is bypassed
        :param debug: debug - False: don't print interim results
        :param http_method: GET by default
        :param timeout: 1200 by default
        :return: 
        '''

        mixpanel_credentials = self.api_credentials
        api_secret = mixpanel_credentials['apisecret']

        if methods[0] == 'export' and len(methods) == 1:
            ''' export api request '''
            request_url = '/'.join([self.EXPORT_ENDPOINT , str(self.VERSION)] + methods)
        else:
            request_url = '/'.join([self.API_ENDPOINT , str(self.VERSION)] + methods)

        if http_method == 'GET':
            data = None
            request_url = request_url + '/?' + self.unicode_urlencode(params)
            if debug:
                print (request_url)
        else:
            data = self.unicode_urlencode(params)

        if maj_ver == 2:
            headers = {'Authorization': 'Basic {encoded_secret}'.format(encoded_secret=base64.b64encode(api_secret))}

        if maj_ver == 3:
            headers = {'Authorization': 'Basic {encoded_secret}'.format(encoded_secret=api_secret)}

        resp = requests.get(request_url , verify=secure, headers=headers, timeout=timeout, stream=True)

        if debug:
            print ("text : {0}".format(resp.text.encode('utf-8')))
            print ("reason : {0}".format(resp.reason.encode('utf-8')))

        if resp.status_code == 200:
            if "export terminated early" in resp.text.encode('utf-8'):
                print ("mixpanel API returned warning - {0}".format(resp.text))
                print ("try again later or contact mixpanel support at support@mixpanel.com")
                raise Warning("unable to export events from mixpanel")
            else:
                buffer = ""
                for chunk in resp.iter_content(chunk_size=1):
                    if chunk.endswith("\n"):
                        buffer += chunk
                        yield buffer
                        buffer = ""
                    else:
                        buffer += chunk

        else:
            print("Response from API - {0}".format(resp.text))
            print("Error Reason - {0}".format(resp.reason))
            print("HTTP Status Code {0}".format(resp.status_code))
            raise Exception("Mixpanel API Request failed")

    def unicode_urlencode(self, params):
        """
            Convert lists to JSON encoded strings, and correctly handle any
            unicode URL parameters.
        """
        if isinstance(params, dict):
            params = params.items()
        for i, param in enumerate(params):
            if isinstance(param[1] , list):
                params[i] = (param[0], json.dumps(param[1]),)

        if maj_ver == 2:
            return urllib.urlencode(
                [(k, isinstance(v, unicode) and v.encode('utf-8') or v) for k, v in params]
            )

        if maj_ver == 3:
            return urllib.parse.urlencode(
                [(k, isinstance(v, str) and v.encode('utf-8') or v) for k, v in params]
            )

        return

    def run_mixpanel_query_file(self, script_file, output_file, secure=False, pretty=False, debug=False):

        mixpanel_credentials = self.api_credentials
        api_secret = mixpanel_credentials['apisecret']

        if secure:
            secure_flag = ""
        else:
            secure_flag = "--insecure"

        if pretty:
            pretty_format = ""
        else:
            pretty_format = " | python -m json.tool"

        url_endpoint = '/'.join([self.API_ENDPOINT, str(self.VERSION), 'jql'])

        rest_api_call = \
            """curl {URL} {secure_flag} -u {secret_key}: --data-urlencode script@{script_file} \
            {pretty_str} > {output_file}""".format(URL=url_endpoint, secret_key=api_secret,
                                                   pretty_str=pretty_format,
                                                   secure_flag=secure_flag,
                                                   script_file=script_file,
                                                   output_file=output_file)
        if debug:
            print(rest_api_call)

        output, error = subprocess.Popen(rest_api_call, shell=True, stderr=subprocess.STDOUT,
                                         stdout=subprocess.PIPE).communicate()
        if sys.version_info.major == 3:
            output = output.splitlines()
            for line in output:
                print(str(line, 'utf-8'))
        else:
            print(output)
        return


