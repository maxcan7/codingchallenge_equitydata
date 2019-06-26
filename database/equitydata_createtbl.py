#!/usr/bin/env py

import sys
import psycopg2
from configparser import ConfigParser


def config(confpath, section):
    '''
    Function to load config files
    '''
    parser = ConfigParser()  # create a parser
    parser.read(confpath)  # read config file
    conf = {} # Load conf dictionary
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            conf[param[0]] = param[1]
    else:
        raise Exception(
            'Section {0} not found in the {1} file'.format(section, confpath))
    return conf


def create_tables():
    """ create tables in the PostgreSQL database"""
    csv_file_name = config['datapath']
    sql = "COPY equity_data FROM STDIN DELIMITER '|' CSV HEADER"
    conn = None
    try:
        params = config()  # read the connection parameters
        conn = psycopg2.connect(**params)  # connect to the PostgreSQL server
        cur = conn.cursor()  # create cursor object
        cur.copy_expert(sql, open(csv_file_name, "r"))  # Copy csv as table to db using cursor obect
        cur.close()  # Close cursor
        conn.commit()  # commit the changes
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    dataconf = config(sys.argv[1], 'config')
    create_tables()
