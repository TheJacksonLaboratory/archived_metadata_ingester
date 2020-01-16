#!/usr/bin/env python

# Given identifying context fields that typically occur in our metadata, such as
# managerUserId, userName, investigatorID, etc, find the associated system_group(s).

import argparse
import configparser
import pymongo
import re

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
        self.cc_groups_collection = self.mdb_connection[mdb["database"]][mdb["cc_groups_collection"]]
        self.metadata_collection = self.mdb_connection[mdb["database"]][mdb["collection"]]

        # These are all the commonly occuring keys in our metadata docs that can establish identity.
        self.identifying_keys = set(["fs_lab", "group", "groupname", "investigatorid", "investigatorname", "manageruserid",
            "system_group", "system_groups",  "userid", "username"])

        # Some values or the identifying fields don't actually tell us anything, so ignore them.
        self.ignore_vals = set(["", "na", "dnf", "jaxadmin", "none", "researchit", "root"])

        # Omero docs will require special handling. They are most easily distinguished by their
        # archivedPath
        self.omero_path = self.config["imaging"]["omero_path"]

        # Make additonal lookup tables to search by PI surname and userid     
        self.build_surname_and_userid_lookup_tables()

        # Pre-compile this regex for searching archivedPaths for lab names.
        self.lab_search_regex = re.compile(self.config["group_search"]["lab_pattern"], re.IGNORECASE)

        # some group names are specified in their own section of the config file
        self.odd_groupnames_table = self.build_odd_groupname_lookup_table()

        # Get a default user (jaxuser) to assign when we can't find anything else.
        self.default_user = self.config["default_system_group"]["group_name"]
        
    # Iterate over all metadata docs in the metadata collection. For each, iterate over keys, 
    # and if a key is one of the identifying_keys, attempt to use it and its value to lookup
    # the system groups. If we find something useful for that doc, stop and go on to the next.
    def add_groups_to_all_docs(self):
        for doc in self.metadata_collection.find({}):
            found_groups = False
            # Many documents will not have sufficent data to even try a lookup.
            # We need to know which ones.
            found_useable_query = False 
            for key in doc.keys():
                newkey,tmp = self.transform_context_strings(key, "")
                if newkey in self.identifying_keys:
                    tmp, val = self.transform_context_strings("", doc[key])
                    if val not in self.ignore_vals:
                        found_useable_query = True
                        sys_groups = self.get_system_groups(newkey, val)
                        if sys_groups and sys_groups[0] not in self.ignore_vals:
                            print(f'For archived path {doc["archivedPath"]}, key {key} and val {val} yielded system_groups {sys_groups}.')
                            # Stop searching if we found a valid group
                            found_groups = True
                            break
            if found_groups:
                # All set, nothing else to do for this doc
                continue
            else:
                # If we haven't found a system_group, try the special cases
                sys_groups = self.add_groups_to_special_case_docs(doc)
                if sys_groups:
                    print(f'For special case archived path {doc["archivedPath"]}, found system_groups {sys_groups}.')
                    continue

            if not sys_groups: 
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

    # Some docs have known exceptions that must be handled separately.
    def add_groups_to_special_case_docs(self, doc):
        if doc['archivedPath'].startswith( self.omero_path):
            sys_groups = [ "jaxuser" ]
            return sys_groups

    # Make a lookup table for groups that were odd or unusual cases
    def build_odd_groupname_lookup_table(self):
        table = {}
        for key,val in self.config["group_name_odd_cases"].items():
            # The key in this dict will actually be the values when we try to search for groups,
            # so they need to be transformed as values are, not as keys are.
            tmp,new_key = self.transform_context_strings("", key)
            table[new_key] = val
        return table
            

    # In hundreds of cases, we can only identify data because a PI's surname or userid appears in the
    # archivedPath. Build a lookup table of those for brute-force string matching, to be used when all 
    # else fails.
    def build_surname_and_userid_lookup_tables(self):
        self.surnames_to_groups = dict()
        self.userids_to_groups = dict()
        
        for doc in self.cc_groups_collection.find({}):
            surname = doc["surname"].lower()
            userid = doc["surname"].lower()
            #sys_groups = self.transform_context_strings("", doc["system_groups"])
            sys_groups = doc["system_groups"]

            self.surnames_to_groups[surname] = sys_groups
            self.userids_to_groups[userid] = sys_groups


    # Given a context's key and value, such as { "userName", "jbancher" } or
    # { "investigatorId", "Roel Verhaak"} , translate the key into a key or keys found in the
    # cost center dump. Transform the value as needed to match the format of known values.
    def get_query_from_context(self, key, val):
        query = None
        key, val = self.transform_context_strings(key, val)

        if key in [ "investigatorid", "manageruserid", "userid", "username" ]:
            query = { "userid": val}

        elif key in [ "fs_lab", "group", "groupname", "system_group", "system_groups"]:
            if val in self.odd_groupnames_table:
                val = self.odd_groupnames_table[val]
            query = { "system_groups": val }

        elif key in [ "investigatorname", "pi" ]:
            # This will be a PIs first name and last name, separated by a space.
            # Before we treat the normal cases for PIs, we have to check the odd ones.
            if val in self.odd_groupnames_table:
                # We already know the group for these cases.
                val = self.odd_groupnames_table[val]
                query = { "system_groups": val }
                return query

            fields = val.split(" ")
            # Anything that's not two words separated by a space is not recognized.
            # Examples of unwanted values are "GT-test" and "SAT-run"
            if len(fields) == 2:
                firstname = fields[0]
                surname = fields[1]
                query = { "firstname": firstname, "surname": surname}
            else:
                return None

        elif key in [ "surname", "lastname" ]:
            query = { "surname": val }

        assert(query)
        return query      

    # Attempt to lookup the given key and value in the mongodb
    def get_system_groups(self, key, val):
        query = self.get_query_from_context(key, val)
        cc_entry = self.cc_groups_collection.find_one(query)
        sys_groups = None
        if cc_entry: # Found a doc
            sys_groups = cc_entry["system_groups"]

        return sys_groups
        

    # Test whether an archivedPath contains any PI's surname,
    # using the lookup tables built during init.
    def search_archived_path_for_group_name(self, archived_path):
        #archived_path = archived_path.lower() #= self.transform_context_strings("", archived_path)

        # The algorithm here is to split the path by directory (forward slashes), and see which 
        # part of the path has the word "lab" in it, case insensitive. Then see if a PI surname 
        # can be found in the same part, before that occurance. The context key ("investigatorname", 
        # "surname", etc) will be determined by whether we find a firstname, lastname, both or neither.
        key,val,sys_groups=(None,None,None)
        fields  = archived_path.split('/')
        for field in fields:
             # Search each field for the word
             match = re.search(self.lab_search_regex, field)
             if match:
                 lab_pos = match.span()[0] # position where lab regex appears 
                 # Search only the part of the string before that.         
                 subfield = field[:lab_pos]
                 # If there are two capital letters in the subfield, they represent
                 # the PI's first and last name. We need to separate them with a space.
                
                 caps_idxs = [m.start(0) for m in re.finditer(r"[A-Z]", subfield)]
                 if len(caps_idxs) == 2:
                      surname_pos = caps_idxs[1]
                      # The context that uses a PIs first and last name is the "investogatorname"
                      key = "investigatorname"
                      val = subfield[:surname_pos] + ' ' + subfield[surname_pos:]
                      
                 else:
                     # We only have the surname
                     key = "surname"
                     val = subfield
             
                 if key and val:
                     sys_groups = self.get_system_groups(key, val)
                     if not sys_groups:
                         # Sometimes the lab regex occurs after a group name, rather than a PI surname, 
                         # such as with grs-lab. In this case, change the key to "group", append the 'lab'
                         # tag back on, and try again.
                         key = "group"
                         val += "-lab"

                         
                         sys_groups = self.get_system_groups(key, val)
                 if sys_groups:
                     return sys_groups
        return None


    # Perform basic string transforms on keys and values that will improve the 
    # accuracy and efficiency of searches in mongo. Regex searches are slower and 
    # introduce more room for error, and wrong case or mixed hyphens and underscores
    #  can fail to find results, so we change the strings to eliminate the need
    # for those.
    def transform_context_strings(self, key, val):
        if key == None:
            key = ""
        if val == None:
            val = ""

        # Convert everything to lowercase and remove trailing or leading whitespace
        key = key.lower().strip()
        val = val.lower().strip()
        
        # The keys should only have underscores, but the values should only have
        # hyphens
        key = key.replace('-', '_')
        val = val.replace('_', '-')

        # If the val has a space in it but the 2nd word starts with 'lab', change the space to
        # a hyphen, and truncate anything after 'lab'.
        fields = val.split(' ')
        if (len(fields) == 2) and fields[1].startswith('lab'):
            val = fields[0] + "-lab"

        return key,val


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Iterate over all metadata docs in mongoDB, add system_groups to each", prog="AddSystemGroupsToMetadata.py")
    parser.add_argument("-m", "--mode", help="prod or dev", default="dev", type=str)
    args = parser.parse_args()

    asgm = AddSystemGroupsToMetadata(args.mode)
    asgm.add_groups_to_all_docs()


