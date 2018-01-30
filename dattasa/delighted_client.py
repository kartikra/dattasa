import json
import requests
import sys
import urllib
from base64 import b64encode

maj_ver = sys.version_info.major
if maj_ver == 2:
    import urllib2


class DelightedClient():

    '''
    https://delighted.com/docs/api/
    '''

    API_ENDPOINT = 'https://api.delighted.com'
    VERSION = 'v1'

    def __init__(self, api_credentials):

        self.api_credentials = api_credentials

        ''' Password field is blank. Leave 2nd parameter after : as blank  '''
        encoded_api_key = (self.api_credentials['apikey'] + ":").encode("ascii")

        ''' Pass the api_key as username as a byte object, encode it into base64 and decode it back to ascii '''
        headers = {"Authorization": "Basic " + b64encode(encoded_api_key).decode("ascii")}
        self.api_header = headers

        return

    def unicode_urlencode(self, params):
        """
            Convert lists to JSON encoded strings, and correctly handle any
            unicode URL parameters.
        """
        if isinstance(params , dict):
            params = params.items()
        for i, param in enumerate(params):
            if isinstance(param[1] , list):
                params[i] = (param[0], json.dumps(param[1]),)

        if maj_ver == 2:
            return urllib.urlencode(
                [(k , isinstance(v, unicode) and v.encode('utf-8') or v) for k, v in params]
            )

        if maj_ver == 3:
            return urllib.parse.urlencode(
                [(k, isinstance(v, str) and v.encode('utf-8') or v) for k, v in params]
            )

        return

    def get_metrics(self, params={}, verify=False):

        method_name = "metrics"
        url_endpoint = self.API_ENDPOINT + "/" + self.VERSION + "/" + method_name + ".json"
        my_session = requests.session()

        if params == {}:
            r = my_session.get(url_endpoint, verify=verify, headers=self.api_header)
        else:
            r = my_session.get(url_endpoint + "?" + self.unicode_urlencode(params),
                               verify=verify, headers=self.api_header)

        return r.json()

    def request_data(self, method_name, params={}, verify=False):

        if method_name not in ("unsubscribes", "bounces", "survey_responses"):
            print ("method_name should be 1 of unsubscribes, bounces, metrics or survey_responses")
            raise Exception("Invalid method_name = " + method_name)

        response_list = []
        url_endpoint = self.API_ENDPOINT + "/" + self.VERSION + "/" + method_name + ".json"
        my_session = requests.session()

        i = 0
        more_pages = True
        while more_pages:
            i += 1
            params["page"] = i
            params["per_page"] = 100
            api_response = my_session.get(url_endpoint + "?" + self.unicode_urlencode(params),
                                          verify=verify, headers=self.api_header)

            if len(api_response.json()) > 0:
                for response in api_response.json():
                    response_list.append(response)
            else:
                more_pages = False

        return response_list
