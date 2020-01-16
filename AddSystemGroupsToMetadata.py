#!/usr/bin/env ptyhon

# For every metadata document in the dev or prod mongoDB, attempt to find
# a system group and update the document with it.

import GroupFinder

class AddSystemGroupsToMetadata:
    # Connect to the mongoDB and get the "cost_center_groups" collection.
    # It has all the cost center info, with system groups added.
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
        self.metadata_collection = self.mdb_connection[mdb["database"]][mdb["collection"]]
        self.lookup_groups 

    # Iterate over all metadata docs in the metadata collection. For each, iterate over keys, 
    # and if a key is one of the identifying_keys, attempt to use it and its value to lookup
    # the system groups. If we find something useful for that doc, stop and go on to the next.
    def add_groups_to_all_docs(self):
        for doc in self.metadata_collection.find({}):
            for key in doc.keys():
                system_groups = self.lookup_system_group()
            if found_groups:
                # All set, nothing else to do for this doc
                continue

            # Found nothing in the metadata. Try searching the archived_path
            sys_groups = self.search_archived_path_for_group_name(doc["archivedPath"])
            if sys_groups:
                print(f'For path search of archived path {doc["archivedPath"]}, found system_groups {sys_groups}.')
                continue

            # Found nothing, note whether the query was at least valid
            if found_useable_query:
                print(f'No group found for {doc["archivedPath"]}.')
            else:
                print(f'No query found for {doc["archivedPath"]}.')



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Iterate over all metadata docs in mongoDB, add system_groups to each", prog="AddSystemGroupsToMetadata.py")
    parser.add_argument("-m", "--mode", help="prod or dev", default="dev", type=str)
    args = parser.parse_args()

    asgm = AddSystemGroupsToMetadata(args.mode)
    asgm.add_groups_to_all_docs()

