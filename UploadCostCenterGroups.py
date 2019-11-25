#!/usr/bin/env python

# Upload cost centers with system groupos to mongodb

import argparse
import configparser
import json
import pymongo
import sys

import MapCostCenterToSystemGroups

class UploadCostCenterGroups:
    # Can run in prod or dev mode, default is dev
    def __init__(self, mode : str = "dev"):

        # Load config and secrets file
        self.config = configparser.ConfigParser()
        self.config.read("config.cfg")

        self.secrets = configparser.ConfigParser()
        self.secrets.read("secrets.cfg")

        assert mode in ["dev", "prod"]
        if mode == "dev":
            mongodb_mode = "mongodb_dev"
        else:
            mongodb_mode = "mongodb_prod"

        # Connect to the mongoDB 
        mdb = self.config[mongodb_mode]
        smdb = self.secrets[mongodb_mode]
        self.mdb_connection = pymongo.MongoClient(
            mdb["address"],
            int(mdb["port"]),
            username = mdb["username"],
            password = smdb["password"],
            authSource = mdb["authSource"],
        )
        self.collection = self.mdb_connection[mdb["database"]][mdb["cc_groups_collection"]]
        self.index_key = self.config["index_names"]["cost_center_index"]
        self.collection.create_index([(self.index_key, pymongo.ASCENDING)], unique=True)

    def upload_cost_centers(self):
        map_cc2groups = MapCostCenterToSystemGroups.MapCostCenterToSystemGroups()
        cc_groups = map_cc2groups.get_cost_centers_with_groups()
        for cc_entry in cc_groups:
            cc_name =  cc_entry["cc_name"]
            try:
                result = self.collection.insert_one(cc_entry)
                if result.acknowledged:
                    print("Inserted " + cc_name, flush=True)
                else:
                    sys.stderr.write(
                        f"cost center entry {cc_name} could not be inserted, skipping.\n"
                    )
                    sys.stderr.flush()

            except Exception as e:
                sys.stderr.write(
                    f"Cannot ingest cost center entry {cc_name}, received exception {str(e)}.\n"
                )
                sys.stderr.flush()



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload cost center entrie with their system groups into mongodb", prog="UploadCostCenterGroups.py")
    parser.add_argument("-m", "--mode", help="prod or dev", default="dev", type=str)
    args = parser.parse_args()

    up_ccg = UploadCostCenterGroups(args.mode)
    up_ccg.upload_cost_centers()



