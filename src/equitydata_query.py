#!/usr/bin/env py
import configpath as cfgpath
from configparser import ConfigParser
from sqlalchemy import create_engine
import csv
import os
import shutil


def load_config(configpath, section):
    '''
    Use configparser to load the .ini file
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
    if section == 'config':  # Specific preprocessing for config section
        config['factors'] = config['factors'].split(',')  # Split factors
        config['types'] = config['types'].split(',')  # Split types
        config['bins'] = list(map(int, config['bins'].split(',')))  # Split bins and convert from string to int
    return config


def data_prep(SQLconfig, dataconfig):
    '''
    Connect python to postgres using sqlalchemy and do some additional data cleaning
    '''
    engine = create_engine(SQLconfig['engine'])  # Create postgresql engine
    conn = engine.raw_connection()  # Create psycopg2-like raw connection
    cur = conn.cursor()  # Create cursor object
    cur.execute("DELETE FROM equity_data WHERE equity_data." + dataconfig['outcome'].lower().replace(" ", "") + " IS NULL;")  # Delete rows with null outcomes
    for f in dataconfig['factors']:
        cur.execute("DELETE FROM equity_data WHERE equity_data." + f.lower().replace(" ", "") + " IS NULL;")  # Delete rows with null factor f
    cur.close()
    conn.commit()
    conn.close()


def run_query(SQLconfig, dataconfig):
    '''
    Connect python to postgres using sqlalchemy and run query.
    '''
    engine = create_engine(SQLconfig['engine'])  # Create postgresql engine
    conn = engine.raw_connection()  # Create psycopg2-like raw connection
    cur = conn.cursor()  # Create cursor object
    factor = dataconfig['factors'][0].lower().replace(" ", "")  # Factor
    bindat = factor + '_bins'  # Binned Factor Label
    query = "SELECT AVG(" + dataconfig['outcome'].lower().replace(" ", "") + ") FROM equity_data GROUP BY " + bindat + ";"
    cur.execute(query)  # Execute binning command
    result = cur.fetchall()  # Fetch query results
    outfile = open(bindat + '_SQL.csv', 'w')  # Open csv to write results
    outcsv = csv.writer(outfile)  # Create csv writer object
    outcsv.writerow(x[0] for x in cur.description)  # Write Columns
    outcsv.writerows(result)  # Write Rows
    outfile.close()
    cur.close()
    conn.commit()
    conn.close()
    shutil.move(dataconfig['mainpath'] + '/src/' + bindat + '_SQL.csv', dataconfig['mainpath'] + bindat + '_SQL.csv')


if __name__ == '__main__':
    SQLconfig = load_config(cfgpath.configpath, 'postgresql')
    dataconfig = load_config(cfgpath.configpath, 'config')
    data_prep(SQLconfig, dataconfig)
    run_query(SQLconfig, dataconfig)
    