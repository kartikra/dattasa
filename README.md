# Project Background
### python package that helps data engineers and data scientists accelerate data-pipeline development 
The goal of this python project is to build a bunch of wrappers that can be reused for building data pipelines from -
- Relational databases: postgres, mysql, greenplum, redshift, etc.
- NOSQL databases: hive, mongo, etc.
- messaging sources and caches: kafka, redis, rabbitmq, etc.
- cloud service providers: salesforce, mixpanel, jira, google-drive, delighted, wootric, etc.

## Installation
### There are 3 ways to install dattasa package -

1) Easiest way is to install from pypi using pip
```
pip install dattasa
```
2) Download from github and build from scratch
```
git clone git@github.com:kartikra/dattasa.git
cd dattasa
python setup.py build
python setup.py clean
python setup.py install
```
3) Download from github and install using pip
```
git clone git@github.com:kartikra/dattasa.git
cd dattasa
pip install -e .
pip install -U -e . (if upgrading)
```

## Config Files
By default dattasa expects the config files to be in the mode directory of user.
These can be overridden. See links to sample code in README file below to find out more.
There are 2 yaml config files
- database.yaml - conists of database credentials and api keys needed for making connection. see [sample database config](documentation/database.yaml)
- ftpsites.yaml - Needed for performing sftp transfers. see [sample ftpsites config](documentation/ftpsites.yaml)

## Environment Variables
dattasa package relies on the following environment variables. Make sure to set these in your bash profile
- GPLOAD_HOME: Path to gpload package (needed only if using gpload utilities for greenplum or redshift)
- PROJECT_HOME: Path to python project directory 
- PROJECT_HOME/python_bash_scripts: python scripts to invoke gpload (needed only if using gpload utilities for greenplum or redshift)
- SQL_DIR: Place to keep all sql scripts
- TEMP_DIR: All temp files created in this folder
- LOG_DIR: All log files are created in this folder


## Description of classes and methods
v1.0 of the package comprises of the following classes. Please see link to sample code for details on how to use each of them.

|class|  Description  | Sample Code |
|:--------|:-----------------|:---------|
environment| Lets you source all the os environment variables|
postgres_client| Lets you use psql and gpload utilities provided by [pivotal greenplum](https://gpdb.docs.pivotal.io/4350/common/client-docs-unix.html). Make connections to postgres / greenplum database using pyscopg2 or sqlalchemy.Use the connections to interact with database in interactive program or run queries from a sql file using the connection|
mysql_client|Lets you use mysql and other methods provided by PyMySQL Package|
file_processor|Create sftp connection using [paramiko](https://github.com/paramiko/paramiko.git) package. Other file manipulations like row_count, encryption, archive (File Class)|
notification|Send email notifications|
mongo_client|Load data to mongodb using bulk load. Run java script queries|
redis_client|Read data from a redis cache or load a redis cache|
kafka_system|Currently allows Publisher and Consumer to use kafka in batch mode|
rabbitmq_system|Currently has Publisher to publish messages in rabbitmq|
mixpanel_client|Connect to mixpanel api and fetch data using jql or export raw events data. [mixpanel api documentation](https://mixpanel.com/help/reference/jql/api-reference)|
salesforce_client|Create a connection to salesforce using [simple_salesforce](https://github.com/simple-salesforce/simple-salesforce) package|
delighted_client|Get nps scores and survey responses from delighted.[api documentation](https://delighted.com/docs/api/)|
wootric_client|Gets nps scores and survey responses from wootric.[api documentation](http://docs.wootric.com/api)|
dag_controller|Functions needed to integrate this package within an airflow dag. [airflow documentation](https://airflow.apache.org/) and [github project](https://github.com/apache/incubator-airflow)|

### data_pipeline class
This is the main class that's accessible to other projects. 
The data pipeline consists of data from components and API.
Each object of data-processor can use individual data streams and process them data_pipeline decides which
modules to call based on type of database (as defined in config file). 
data_pipeline comprises of 3 classes
- DataComponent : Each database connection is considered to be data-component object
- APICall : Each api call is an apicall object.
- DataProcessor : transfers and loads data between data components
