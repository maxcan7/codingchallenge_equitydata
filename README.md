# codingchallenge_equitydata
This coding challenge uses capital market data (not provided), but is flexible to other zipped csv inputs so long as the configuration is correctly set.

The purpose of this pipeline is to take a .zip file with a .csv dataset and take the average of an outcome variable, grouped by one or more factors binned into assigned number of bins.

I chose to preprocess the data in python3 using pandas and then store the dataframe as a table in a postgres SQL database. However, there is also a configuration option to output the query as a csv from python using pandas.

I have a windows PC and git bash has been finicky for unix terminal operations for me so I chose to write this pipeline almost exclusively in python, run through Spyder / Ipython. With more time, I would like to test this dataset in an Ubuntu virtualmachine and run it through a unix terminal.

Additionally, with more time I would like to write a test suite for unit testing.

## Requirements
This pipeline requires python3 and postgres

**The following python packages are required for equitydata_createdb.py**:  
configparser  
psycopg2  

**The following python packages are required for equitydata_preprocess.py**:  
zipfile  
configparser  
pandas  
numpy    
sqlalchemy  

**The following python packages are required for equitydata_query.py**:  
configparser  
sqlalchemy  
csv  

## Config
You will need to create a .ini file as a config with the following sections:

**[config]**  
mainpath=path to cloned directory  
datapath=path to zipped data file  
factors=which variable in your dataset to use as the factor. The pipeline currently only supports averaging over one factor.  
outcome=which variable in your dataset to use as the outcome measure to be averaged over. This must be a continuous (numeric) variable.  
types=the data type of the factor. The pipeline currently supports continuous (type=float), categorical (type=categorical), and datetime (type=datetime).  
bins=the number of bins to separate the factor into. If the factor is already categorical, this is ignored.     
dosql=whether to write the results to a csv in the main directory from python (dosql=0) or write the dataframe as a table to a postgres database (dosql=1).   

**[createdb]**  
NOTE: This is only necessary if the dosql parameter of the config section is 1  

defaultdb=the default database to connect  
port=the port for that database  
user=preferred postgres user  
password=postgres password  
host=127.0.0.1 or server address  

**[postgresql]**  
NOTE: This is only necessary if the dosql parameter of the config section is 1  

host=localhost or server name  
database=equitydata_db  
user=preferred postgres user  
password=postgres password  
engine=postgresql://USER:PASSWORD@HOST/equitydata_db  
engine is used for SQLalchemy, the other parameters are used for psycopg2  

**configpath.py**  
A python script that only contains a string with the path for the .ini file. My .ini file is in the main directory but it should be flexible.

example:  
'your preferred python shebang'  
configpath = "path/configname.ini"  

## Create Database (equitydata_createdb.py)
NOTE: This is only necessary if the dosql parameter of the config section is 1  

A python script that uses psycopg2 to create the new equitydata_db database in postgres using the info provided in the .ini file "[createdb]" section. I chose to use psycopg2 here based on prior familiarity.

## Preprocess (equitydata_preprocess.py)
A python script to do some simple preprocessing of the data before setting the table in the postgres database. The pipeline unzips the csv file(s), loads them as a dataframe using pandas (concatenating them if multiple files), and does some string formatting on the headers and other preprocessing. 

Note that while the pipeline does support datetime factors, the method I chose to make it flexible to different kinds of datetime formats also makes it slower. In the future, it may be better to either code in a way to parse the format, or add manual formatting to the .ini file.

I chose to use sqlalchemy to write the dataframe as a table in the postgres database. I chose this because there is a simple pandas function to write dataframes to SQL using sqlalchemy, although with larger data this is not efficient (although with larger data pandas is not efficient in general).

If this were a Big Data pipeline, I would use pyspark rather than pandas, and probably use psycopg2 or a spark SQL (in pyspark)

If the parameter dosql in the config section of the .ini file is 0, the preprocessing script will also write a .csv file to the main folder with the result, the subsequent script is only necessary if using the postgres database.

## Query (equitydata_query.py)
NOTE: This is only necessary if the dosql parameter of the config section is 1  

A python script to do additional data preprocessing for the SQL table, and query the average of the outcome variable by the binned factor, and output the results to a csv in the main directory.

I chose to use sqlalchemy for connecting to the postgres database and querying, mainly to get more experience with sqlalchemy. Ultimately I used it as a raw connector more like how one would use psycopg2. With more time, I would like to have used sqlalchemy more for its Object Relational Mapping (ORM).