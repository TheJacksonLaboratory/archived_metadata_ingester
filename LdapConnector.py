#!/usr/bin/env python

# get all groups from LDAP

import configparser
from ldap3 import Server, Connection, ALL, SUBTREE, AUTO_BIND_NO_TLS

class LdapConnector:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read("config.cfg")

        self.secrets = configparser.ConfigParser()
        self.secrets.read("secrets.cfg")

        ldap_conf_dict = self.config["ldap"]
        ldap_server = ldap_conf_dict["server"]
        ldap_connection_conf = ldap_conf_dict["connection_conf"]
        ldap_password = self.secrets["ldap"]["password"]

        self.server = Server(ldap_server, get_info=ALL)
        self.connection = Connection(self.server, ldap_connection_conf, ldap_password, auto_bind=AUTO_BIND_NO_TLS)

    def get_groups(self):
        user = dict()
        user['groups'] = [str(x['cn']) for x in self.connection.entries]
        print("\nGroups: {}".format(user['groups']))

    def run_example_code(self):
        user = dict()
        userid = "Neil.Kindlon@jax.org"
        # search domain for user’s email address
        self.connection.search(search_base='DC=jax,DC=org', search_filter='(mail={0})'.format(userid), search_scope = SUBTREE, attributes=['CN', 'sAMAccountName'])

        # pull out the distinguished name
        dn = self.connection.response[0]['dn']

        # print out user’s short name and distinguished name
        print("User name: " + self.connection.response[0]['attributes']['sAMAccountName'])
        print("\nDistinguished Name: " + dn)

        # use DN to find user’s group memberships
        self.connection.search(search_base='DC=jax,DC=org', search_filter='(&(member={0})(objectClass=group))'.format(dn), search_scope = SUBTREE, attributes=['CN'])

        # pull out groups from the response
        user['groups'] = [str(x['cn']) for x in self.connection.entries]

        # print group output
        print("\nGroups: {}".format(user['groups']))


if __name__ == "__main__":
    ldap = LdapConnector()
    #ldap.get_groups()
    ldap.run_example_code()





        

