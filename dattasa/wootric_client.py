import requests
import sys
import urllib
import json

maj_ver = sys.version_info.major
if maj_ver == 2:
    import urllib2


class WootricClient():
    '''
    http://docs.wootric.com/api
    '''

    API_ENDPOINT = 'https://api.wootric.com'
    VERSION = 'v1'

    def __init__(self, api_credentials):
        self.api_credentials = api_credentials

        form = dict(
            grant_type='client_credentials',
            client_id=api_credentials['apikey'],
            client_secret=api_credentials['apisecret']
        )

        resp = requests.post(self.API_ENDPOINT + "/oauth/token", data=form)

        if resp.status_code != 200:
            raise Exception('Unable to authenticate. Check login credentials ')
        else:
            access_token = resp.json().get("access_token", None)
            self.access_token = access_token

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

    def request_data(self, method_name, params={}, verify=False):

        url_endpoint = self.API_ENDPOINT + "/" + self.VERSION + "/"

        my_session = requests.session()
        headers = {"Authorization": "Bearer " + self.access_token}
        response_list = []

        if method_name not in ("nps_summary", "responses", "end_users", "declines", "segments", "email_survey"):
            print ("Invalid method_name = " + method_name)
            print ("Check http://docs.wootric.com/api/ for list of avialble methods")
            raise Exception("Invalid method_name = " + method_name)

        i = 0
        more_pages = True
        while more_pages:
            i += 1
            params["page"] = i
            params["per_page"] = 50

            api_response = my_session.get(url_endpoint + method_name + "?" + self.unicode_urlencode(params),
                                          verify=verify, headers=headers)
            try:
                if api_response.json()['message'] == 'Not Found':
                    print("curl request - " + url_endpoint + method_name + "?" + self.unicode_urlencode(params))
                    print("no results returned. check curl request")
                    break
            except:
                if len(api_response.json()) > 0:
                    for response in api_response.json():
                        response_list.append(response)
                else:
                    more_pages = False

                if len(api_response.json()) < 50:
                    more_pages = False

        return response_list
