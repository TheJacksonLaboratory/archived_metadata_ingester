#!/bin/usr/env python

# The cost center is used by the HPC admins to bill the various labs and groups
# at JAX. It is a definitive listing of PIs' long names and userids, as well as
# their email addresses and the long name of their lab or department.

import configparser
import json
import os
import requests
import ssl

class CostCenterConnector:
    def __init__(self):

        # Read config and password files
        self.config = configparser.ConfigParser()
        self.config.read("config.cfg")

        self.secrets = configparser.ConfigParser()
        self.secrets.read("secrets.cfg")

        # save variables for readability
        cc_url = self.config["cost_center"]["cc_url"]
        api_key_val = self.secrets["cost_center"]["api_key_val"]

        # Send a request to the cost center, store the response
        self.cost_center_response = requests.get(cc_url,
            headers = {'API-KEY': api_key_val}, verify = False )


    # Return the cost center response as json
    def get_all(self):
        return self.cost_center_response.json()

    def dump_all(self):
        print(json.dumps(self.get_all(), indent=4, sort_keys=True))

if __name__ == "__main__":
    ccc = CostCenterConnector()
    ccc.dump_all()



