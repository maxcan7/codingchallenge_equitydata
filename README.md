# codingchallenge_equitydata
This coding challenge uses capital market data (not provided), but is flexible to other zipped csv inputs so long as the configuration is correctly set.

The purpose of this pipeline is to take a .zip file with a .csv dataset and take the average of an outcome variable, grouped by one or more factors binned into assigned number of bins.

I chose to preprocess the data in python3 using pandas and then store the dataframe as a table in a postgres SQL database.

I have a windows PC and git bash has been finicky for unix terminal operations for me so I chose to write this pipeline almost exclusively in python, run through Spyder / Ipython. With more time, I would like to test this dataset in an Ubuntu virtualmachine and run it through a unix terminal.

## Config
You will need to create a .ini file as a config with the following sections:

**[config]**  
mainpath=path to cloned directory  
datapath=path to zipped data file  
factors=which variables in your dataset to use as factors  
outcome=Returns  
bins=the number of bins to separate each factor into  

**[createdb]**  
defaultdb=the default database to connect  
port=the port for that database  
user=preferred postgres user  
password=postgres password  
host=127.0.0.1 or server address  

**[postgresql]**  
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
A python script that uses psycopg2 to create the new equitydata_db database in postgres using the info provided in the .ini file "[createdb]" section. I chose to use psycopg2 here based on prior familiarity.

## Preprocess (equitydata_preprocess.py)
A python script to do some simple preprocessing of the data before setting the table in the postgres database. The pipeline unzips the csv file(s), loads them as a dataframe using pandas (concatenating them if multiple files), and does some string formatting on the headers. 

I had originally intended to do more of the pipeline in pandas but chose to offload that work to SQL, but this way there is at least a framework to do more of the work in python/pandas if you choose.

I chose to use sqlalchemy to write the dataframe as a table in the postgres database. I chose this because there is a simple pandas function to write dataframes to SQL using sqlalchemy, although with larger data this is not efficient (although with larger data pandas is not efficient in general).

If this were a Big Data pipeline, I would use pyspark rather than pandas, and probably use psycopg2 or a spark SQL (in pyspark)

## Query (equitydata_query.py)
A python script to do additional data preprocessing and factor binning using SQL queries, query the average of the outcome variable by the binned factors, and display the outputs (if run in a python interpreter).

I chose to use sqlalchemy for connecting to the postgres database and querying, mainly to get more experience with sqlalchemy. Ultimately I used it as a raw connector more like how one would use psycopg2. With more time, I would like to have used sqlalchemy more for its Object Relational Mapping (ORM).