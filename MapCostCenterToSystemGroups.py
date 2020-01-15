#!/usr/bin/env python

# Create a mapping of cost center and PI names to their system groups, 
# and print it as JSON.

import json
import CostCenterConnector

# Given a list of cost centers and a manually curated list of special cases for 
# for group, try to match at least one system_group to every cost center.
class MapCostCenterToSystemGroups:
    def __init__(self):
        # Read the special cases file, and create a dict of special-case lab cost center entries 
        # to their system groups.
        # The "cc_lab_special_cases.txt" text file is a manually curated listing of cost center
        # entries with non-standard lab or system group names.
        self.lab_special_case_names = {}
        with open("cc_lab_special_cases.txt", 'r') as f:
            # Read line by line, trimming whitespace, ignoring blank lines or
            # comments, and splitting on tabs. Every line should have two fields.
            for line in f:
                line = line.strip()
                if not line or line[0] == '#':
                    continue
                fields = line.split('\t')
                if (len(fields) != 2):
                    raise ValueError("Line %s does not have two fields. Exiting." % line)
                # The first fields is the cost center name. The second is the system groups, which can be a 
                # comma-delimited list
                cc_name = fields[0]
                # sys_groups can be a comma-delimited list
                sys_groups = fields[1].split(',')
                self.lab_special_case_names[cc_name] = sys_groups

        # Read allpossible groups into a set. 
        self.all_sys_groups = set()
        # The "ent_all.txt" file is a listing of all groups on the HPC
        # system. There are several ways to get this programmatically,
        # but this file was already created, so we're just using it for simplicity
        # for now.
        with open("ent_all.txt", 'r') as f:
            # Read line by line, trimming whitespace, ignoring blank lines.
            for line in f:
                line = line.strip()
                if not line:
                    continue
                self.all_sys_groups.add(line)

    # Return all cost centers with system groups added.
    def get_cost_centers_with_groups(self):
        ccc = CostCenterConnector.CostCenterConnector()
        cc_dump = ccc.get_all()

        for cc_entry in cc_dump:
            cc_name = cc_entry["cc_name"]
            sys_groups = self.get_sys_groups_for_cost_center(cc_name)
            cc_entry["system_groups"] = sys_groups
            # Enforce that all values are lowercase. This will make later queries more efficient than trying
            # to a case insensitive/regex search. 
            for key, value in cc_entry.items():
                if type(value) is not str:
                    continue
                cc_entry[key] = value.lower()
            
        # Sort the list of dicts by surname
        return sorted(cc_dump, key = lambda i : i["surname"])

    # Try to determine what the system group is for a cost center.
    # Either read it from the list of special cases, or try to construct
    # it from the cost center name.
    def get_sys_groups_for_cost_center(self, cc_name):
            if cc_name in self.lab_special_case_names:
                sys_groups = self.lab_special_case_names[cc_name]

                # The special cases are already read in as a list in the initializer method,
                # so do not wrap them as a list again here, b/c we do not want a list of lists.
                return sys_groups
            else:
                # for most centers whose name ends with "Lab", the system group is just the 
                # name in lowercase, with the space replaced by a hyphen.
                if cc_name.endswith("Lab"):
                    sys_group = cc_name.lower().replace(' ', '-')

                if sys_group not in self.all_sys_groups:
                    # some PI's labs don't have the hyphen between the PI's last name
                    # and the word lab. Intstead they're one word
                    sys_group = sys_group.replace('-', '')
                    if sys_group not in self.all_sys_groups:
                        print("FAILED: " + sys_group + " is not in all_sys_groups")
                        return "DNF"
                    else:
                      return [ sys_group ]
                else:
                    return [ sys_group ]

if __name__ == "__main__":
    # Get the cost center groups, add system groups, print as JSON.
    map_cc2groups = MapCostCenterToSystemGroups()
    print(json.dumps(map_cc2groups.get_cost_centers_with_groups(), indent=4))


    

