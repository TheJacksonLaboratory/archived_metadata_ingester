#!/usr/bin/env python

# Create a mapping of system groups to cost center and PI names


import CostCenterConnector


# Read the exceptions file, and create a dict of special-case lab cost center entries 
# to their system groups.

lab_special_case_names = {}
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
        # The first fields is the cost center name. The second is the system group.
        cc_name = fields[0]
        sys_group = fields[1]
        lab_special_case_names[cc_name] = sys_group

all_sys_groups = set()
# Read allpossible groups into a set
with open("ent_all.txt", 'r') as f:
    # Read line by line, trimming whitespace, ignoring blank lines.
    for line in f:
        line = line.strip()
        if not line :
            continue
        all_sys_groups.add(line)


ccc = CostCenterConnector.CostCenterConnector()
cc_dump = ccc.get_dump()

for cc_entry in cc_dump:
    cc_name = cc_entry["cc_name"]
    if cc_name in lab_special_case_names:
        sys_group = lab_special_case_names[cc_name]
        print(cc_name + '\t' + sys_group)
    else:
        # for most centers whose name ends with "Lab", the system group is just the 
        # name in lowercase, with the space replaced by a hyphen.
        if cc_name.endswith("Lab"):
            sys_group = cc_name.lower().replace(' ', '-')

        if sys_group not in all_sys_groups and sys_group != "DNF":
            # some PI's labs don't have the hyphen between the PI's last name
            # and the word lab. Intstead they're one word
            sys_group = sys_group.replace('-', '')
            if sys_group not in all_sys_groups:
                print("FAILED: " + sys_group + " is not in all_sys_groups")
            else:
                print(cc_name + '\t' + sys_group)
        else:
            print(cc_name + '\t' + sys_group)

    

