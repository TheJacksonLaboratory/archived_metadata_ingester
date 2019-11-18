#!/bin/usr/env python

# The cost center is used by the HPC admins to bill the various labs and groups
# at JAX. It is a definitive listing of PIs' long namesand userids, which we'll 
# need to map some documents to system groups (likely done elsewhere).

import configparser
import json
import os
import requests
import ssl

class CostCenterConnector:
    def __init__(self):

        self.config = configparser.ConfigParser()
        self.config.read("config.cfg")

        self.secrets = configparser.ConfigParser()
        self.secrets.read("secrets.cfg")
        '''
        self.cost_center_json = requests.get(self.config["cost_center"]["cc_url"],
            headers={'Authorization':   self.secrets["cost_center"]["api_key_name"]:
                self.secrets["cost_center"]["api_key_val"]  } )
        '''
        '''
        if (not os.environ.get('PYTHONHTTPSVERIFY', '') and
            getattr(ssl, '_create_unverified_context', None)):
                ssl._create_default_https_context = ssl._create_unverified_context
        '''

        cc_url = self.config["cost_center"]["cc_url"]
        api_key_val = self.secrets["cost_center"]["api_key_val"]

        self.cost_center_response = requests.get(cc_url,
            headers = {'API-KEY': api_key_val}, verify = False )

    def dump_all(self):
        print(json.dumps(self.cost_center_response.json(), indent=4, sort_keys=True))

if __name__ == "__main__":
    ccc = CostCenterConnector()
    ccc.dump_all()



