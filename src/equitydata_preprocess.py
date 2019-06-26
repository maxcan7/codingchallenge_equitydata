#!/usr/bin/env py
import configpath as cfgpath
from zipfile import ZipFile
from configparser import ConfigParser
import pandas as pd
from sqlalchemy import create_engine


def load_config(configpath, section):
    '''
    Use configparser to load the .ini file with the paths, factors, outcome, and bins for the factors.
    Expected columns and what they mean are in the readme.
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
        config['bins'] = list(map(int, config['bins'].split(',')))  # Split bins and convert from string to int
    return config


def unzip_data(datapath):
    '''
    Use ZipFile to unzip the .zip file and pandas to load the .csv data.
    '''
    zf = ZipFile(datapath, "r")  # Create ZipFile object
    filelist = zf.namelist()  # Get list of all files
    cnt = 0
    for file in filelist:  # Loop through files
        if cnt == 0:  # If first file, load it as a pandas dataframe
            df = pd.read_csv(zf.open(file))
            cnt += 1
        else:  # If not first file, load as pandas dataframe and append
            df.append(pd.read_csv(zf.open(file)))
    return df


def df_to_sql(config, df):
    '''
    Copy the dataframe to the postgres db using sqlalchemy
    '''
    df.columns = [x.lower().replace(" ", "") for x in df.columns]  # Make column names all lowercase and remove spaces to make SQL queries easier
    SQLconfig = load_config(cfgpath.configpath, 'postgresql')  # Load postgres configuration
    engine = create_engine(SQLconfig['engine'])  # Create postgres engine
    df.to_sql('equity_data', engine)  # Write df to postgres db as table


if __name__ == '__main__':
    config = load_config(cfgpath.configpath, 'config')
    df = unzip_data(config['datapath'])
    df_to_sql(config, df)
