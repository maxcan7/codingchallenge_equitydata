#!/usr/bin/env py
import configpath as cfgpath
from configparser import ConfigParser
from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def load_config(configpath, section):
    '''
    Use configparser to load the .ini file with the default postgres info to
    create the equitydata db
    '''
    parser = ConfigParser()  # Create parser
    parser.read(configpath)  # Read config ini file
    config = {}  # Create empty dictionary for config
    if parser.has_section(section):  # Look for 'config' section in config ini file
        params = parser.items(section)  # Parse config ini file
        for param in params:  # Loop through parameters
            config[param[0]] = param[1]  # Set key-value pair for parameter in dictionary
    else:  # Raise exception if the section can't be found
        raise Exception(
            'Section {0} not found in the {1} file'.format(section, configpath))
    return config


def createdb(config):
    '''
    Connect python to postgres using pscyopg2 and create the equitydata db
    '''
    con = connect(database=config['defaultdb'], port=config['port'], 
                  user=config['user'], password=config['password'], 
                  host=config['host'])
    newdb = "equitydata_db"
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    cur.execute('CREATE DATABASE %s ;' % newdb)
    cur.close()


if __name__ == '__main__':
    config = load_config(cfgpath.configpath, 'createdb')
    createdb(config)
