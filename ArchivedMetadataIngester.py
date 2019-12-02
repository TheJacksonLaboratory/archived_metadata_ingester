#!/usr/bin/env python

import argparse
import configparser
import json
import os
import pymongo
import re
import sys

# Find all JSONs beneath specified archive directories, and ingest them into MongoDB.

class ArchivedMetedataIngester:
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

        # Connect to the dataservices database in mongoDB 
        mdb = self.config[mongodb_mode]
        smdb = self.secrets[mongodb_mode]
        self.mdb_connection = pymongo.MongoClient(
            mdb["address"],
            int(mdb["port"]),
            username = mdb["username"],
            password = smdb["password"],
            authSource = mdb["authSource"],
        )

        self.collection = self.mdb_connection[mdb["database"]][mdb["collection"]]

        # The index_key is usually "archivedPath", but could be different. We will need it
        # both here and downstream in the code, so save it here.
        self.index_key = self.config["index_names"]["index_key"]
        self.old_index_keys = (self.config["index_names"]["old_index_keys"]).split(',')
        

        self.collection.create_index([(self.index_key, pymongo.ASCENDING)], unique=True)

        self.MAX_JSON_DOC_SIZE = int(mdb["max_json_doc_size"])

    
    # Ensure the metadata has a valid index key. See comments in the config file.
    def adjust_index_key(self, metadata_dict, json_filename): 
        # If the metadata has one of the older index keys, change it to the new one.
        for old_key in self.old_index_keys:
            if old_key in metadata_dict:
                metadata_dict[self.index_key] = metadata_dict[old_key]
                del metadata_dict[old_key]

        # If the metadata still has no index_key, use the directory in the
        # system where the json was found.
        if self.index_key not in metadata_dict:
            metadata_dict[self.index_key] = os.path.dirname(json_filename)

    # Get all root and sub-directories from the config file
    def crawl_archive_dirs(self):
        # Get every root dir to look at, and the list of sub_dirs for each of them.
        root_dirs_to_paths = dict(self.config["root_dirs"])
        for root_dir, sub_dirs in root_dirs_to_paths.items():
            # sub_dirs are a comma-delimited list of directories
            sub_dirs = sub_dirs.split(",")
            for sub_dir in sub_dirs:
                target_dir = os.path.join(root_dir, sub_dir)
                self.find_jsons(target_dir)
        

    # Crawl a given taget directory to find all json files, pass each
    # to ingest method
    def find_jsons(self, target_dir):
        for top_dir, sub_dirs, files in os.walk(target_dir):
            for filename in files:
                if filename.endswith(".json"):
                    # Get the full path to the file
                    filename = os.path.join(top_dir, filename)
                    self.test_then_ingest(filename)


    # Ingest a metadata doc into mongo.
    def ingest_json(self, metadata_dict, json_filename):
        try:
            result = self.collection.insert_one(metadata_dict)
            if result.acknowledged:
                print("Archived " + json_filename, flush=True)
            else:
                sys.stderr.write(
                    f"metadata {json_filename} could not be inserted, skipping.\n"
                )
                sys.stderr.flush()
                return
        except pymongo.errors.DuplicateKeyError:
            sys.stderr.write(
                f"metadata {json_filename} is already in the collection, skipping.\n"
            )
        except Exception as e:
            sys.stderr.write(
                f"Cannot ingest metadata {json_filename}, received exception {str(e)}.\n"
            )
            sys.stderr.flush()

    # Check to see if the directory containing a gt_metadata file also has a file
    # named archived.json
    def has_gt_post_processed_metadata(self, filename):
        # Get the directory for this file
        dirname = os.path.dirname(filename)
        # Test whether te directory has the file we're looking for
        post_gt_filename = self.config["genome_technologies"]["gt_post_processed_metadata"]
        if os.path.exists(os.path.join(dirname, post_gt_filename) ):
            return True
        else:
            sys.stderr.write(
                f"gt metadata {filename} has no accompanying {post_gt_filename}.\n"
            )
            sys.stderr.flush()
            return False
            
    def is_gt_metadata(self, filename):
        #The gt_metadata.json filename sometimes has mixed case or hyphens instead of underscores
        if re.search(self.config["genome_technologies"]["gt_metadata_pattern"], filename, re.IGNORECASE):
            return True
        else:
            return False
        

    def is_omero_json(self, metadata_dict):
        # Test that the first key in the dict is either 'ndp: ' or 'omereo: ', followed by 6 digitis
        first_key = next(iter(metadata_dict))
        pattern = self.config["imaging"]["omero_pattern"]
        print(f"pattern is {pattern}, key is {first_key}")
        if re.match(pattern, first_key):
            return True
        else:
            return False
        

    def load_json(self, json_filename):
        with open(json_filename, "r") as json_file:
            print(f"Loading file: {json_filename}", flush=True)
            try:
                metadata_dict = json.load(json_file)
                return metadata_dict
            except:
                raise ValueError(f"metadata_file {json_filename} could not be loaded as json, skipping.\n")
                '''
                sys.stderr.write(
                    f"metadata_file {json_filename} could not be loaded as json, skipping.\n"
                )
                sys.stderr.flush()
                return None
                '''


    # iterate over large dictionaries, treating each key and its corresponding sub-dictionary as
    # a separate document.
    def split_json(self, json_filename):
        metadata_dict = self.load_json(json_filename)
        if not self.is_omero_json(metadata_dict):
            sys.stderr.write(
                f"metadata_file {json_filename} is too large and is not a splittable omero json file."
            )
            sys.stderr.flush()
            return
            
        # The directory the filename is in will be the base of the archivedPaths for each document in it.
        dir = os.path.dirname(json_filename)
        for key, val in metadata_dict.items():
            # The val is a dictionary, which is what we want to ingest as a doc.
            # Add they key to the directory where the file was found, and use it as
            # our archivedPath. Turn the ': " in the key to an underscore.
            new_path = os.path.join(dir, key.replace(": ", "_"))
            val[self.index_key] = new_path
            self.ingest_json(val, new_path)


    # There are a number of special cases to look out for before metadata can be ingested
    def test_then_ingest(self, filename):
            # Special: some directories still contain original
            # metadata from GT. We want to skip these and use the
            # archived.json file which should be in the directory instead.
            if (self.is_gt_metadata(filename) and
                    self.has_gt_post_processed_metadata(filename)):
                return

            # Test whether the json file needs to be split b/c its too big
            if os.path.getsize(filename) > self.MAX_JSON_DOC_SIZE :
                self.split_json(filename)
            else:
                metadata_dict = self.load_json(filename)

                # Some files may have an out-dated index field
                self.adjust_index_key(metadata_dict, filename)
                
                # Now we're good, ingest.
                self.ingest_json(metadata_dict, filename)
 
    def just_patch(self):
        for filename in self.patch_list:
            self.test_then_ingest(filename)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest metadata from the archive into mongodb", prog="ArchivedMetadataIngester.py")
    parser.add_argument("-m", "--mode", help="prod or dev", default="dev", type=str)
    args = parser.parse_args()

    ami = ArchivedMetedataIngester(args.mode)
    ami.crawl_archive_dirs()


