#!/usr/bin/env py
import configpath as cfgpath
from zipfile import ZipFile
from configparser import ConfigParser
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import os
import shutil


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
        config['types'] = config['types'].split(',')  # Split types
        config['bins'] = list(map(int, config['bins'].split(',')))  # Split bins and convert from string to int
    return config


def unzip_data(datapath):
    '''
    Use ZipFile to unzip the .zip file and pandas to load the .csv data
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


def bin_factors(config, df):
    '''
    Bin factors based on their type and add to dataframe. Currently only supports
    averaging over bins of a single factor. Currently only supports factors that 
    are continuous (float), categorical (categorical), or datetime (datetime)
    '''
    df = df.dropna()  # Currently just dropping any observations with missing data
    factor = config['factors'][0]  # Factor
    dtype = config['types'][0]  # Datatype for factor
    if dtype != 'categorical':
        nbin = config['bins'][0] + 1  # Number of Bins (bins is nbin-1)
    if dtype == 'float':
        df[factor] = df.loc[:, factor].str.replace(',', '').astype(float)  # Remove commas and convert to float
        bins = np.linspace(df[factor].min()-0.01, df[factor].max(), num=nbin)  # Create bins as linear spacing of factors by bins
        bindat = factor + '_bins'
        df[bindat] = pd.cut(df[factor], bins).astype(str)  # Add binned data column to df
    elif dtype == 'categorical':
        bins = np.unique(df[factor])
    elif dtype == 'datetime':
        #nbin -= 1  # Not using numpy linspace for datetime binning
        df[factor] = pd.to_datetime(df[factor])  # Convert a field such as MM/DD/YYYY to a datetime Series
        bins = pd.date_range(start=df[factor].min() - pd.Timedelta(seconds=1), end=df[factor].max(), periods=nbin)
        bindat = factor + '_bins'
        df[bindat] = pd.cut(df[factor], bins).astype(str)  # Add binned data column to df
    return df


def df_to_sql(config, df):
    '''
    Copy the dataframe to the postgres db using sqlalchemy
    '''
    df.columns = [x.lower().replace(" ", "") for x in df.columns]  # Make column names all lowercase and remove spaces to make SQL queries easier
    SQLconfig = load_config(cfgpath.configpath, 'postgresql')  # Load postgres configuration
    engine = create_engine(SQLconfig['engine'])  # Create postgres engine
    df.to_sql('equity_data', engine)  # Write df to postgres db as table


def write_output(config, df):
    '''
    If not using a postgres SQL database, the output can be written to a csv using pandas
    '''
    outcome = config['outcome']  # Outcome
    factor = config['factors'][0]  # Factor
    bindat = factor + '_bins'  # Binned Factor Label
    avgs = df.groupby(bindat, as_index=False)[outcome].mean()  # Return mean of outcome variable grouped by factors
    avgs.to_csv(bindat + '.csv')
    shutil.move(config['mainpath'] + '/src/' + bindat + '.csv', config['mainpath'] + bindat + '.csv')


if __name__ == '__main__':
    config = load_config(cfgpath.configpath, 'config')
    df = unzip_data(config['datapath'])
    df = bin_factors(config, df)
    if config['dosql'] == '1':
        df_to_sql(config, df)
    else:
        write_output(config, df)
