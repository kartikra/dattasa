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


## Description of classes
v1.0 of the package comprises of the following classes. Please see link to sample code for details on how to use each of them.

|class|  Description  | Sample Code |
|:--------|:-----------------|:---------|
environment| Lets you source all the os environment variables|[see first row in mongo example](documentation/mongo_example.ipynb)
postgres_client|Lets you make connections to postgres / redshift database using pyscopg2 or sqlalchemy.Use the connections to interact with database in interactive program or run queries from a sql file using the connection|[sample postgres code](documentation/postgres_client.ipynb)
greenplum_client (inherits postgres_client)| Lets you use psql and gpload utilities provided by [pivotal greenplum](https://gpdb.docs.pivotal.io/4350/common/client-docs-unix.html). Make connections to postgres / greenplum database using pyscopg2 or sqlalchemy.Use the connections to interact with database in interactive program or run queries from a sql file using the connection|[sample greenplum code](documentation/greenplum_client.ipynb)
mysql_client|Lets you use mysql and other methods provided by PyMySQL Package|[sample mysql code](documentation/mysql_client.ipynb)
file_processor|Create sftp connection using [paramiko](https://github.com/paramiko/paramiko.git) package. Other file manipulations like row_count, encryption, archive (File Class)|[see file processing example](documentation/file_processing.ipynb)
notification|Send email notifications|
mongo_client|Load data to mongodb using bulk load. Run java script queries|[see mongo example](documentation/mongo_example.ipynb)
redis_client|Read data from a redis cache or load a redis cache|[see redis example](documentation/redis_example.ipynb)
kafka_system|Currently allows Publisher and Consumer to use kafka in batch mode|[see kafka example](documentation/kafka_example.ipynb)
rabbitmq_system|Currently has Publisher to publish messages in rabbitmq|
mixpanel_client|Connect to mixpanel api and fetch data using jql or export raw events data. [mixpanel api documentation](https://mixpanel.com/help/reference/jql/api-reference)|[see mixpnael section in api example](documentation/api_examples.ipynb)
salesforce_client|Create a connection to salesforce using [simple_salesforce](https://github.com/simple-salesforce/simple-salesforce) package|[see salesforce section in api example](documentation/api_examples.ipynb)
delighted_client|Get nps scores and survey responses from delighted.[api documentation](https://delighted.com/docs/api/)|[see delighted section in api example](documentation/api_examples.ipynb)
wootric_client|Gets nps scores and survey responses from wootric.[api documentation](http://docs.wootric.com/api)|[see wootric section in api example](documentation/api_examples.ipynb)
dag_controller|Functions needed to integrate this package within an airflow dag. [airflow documentation](https://airflow.apache.org/) and [github project](https://github.com/apache/incubator-airflow)|


### data_pipeline classses
This file comprises of the classes that's accessible to other projects. 
data_pipeline uses a factory design pattern and creates the object based on type of database or api being accessed.
The data pipeline consists of data from components and API.
Each object of data-processor can use individual data streams and process them data_pipeline decides which
modules to call based on type of database (as defined in config file). 
data_pipeline comprises of 3 classes
- DataComponent : Each database connection is considered to be data-component object.See examples for postgres, mysql, greenplum, etc above
- APICall : Each api call is an apicall object. See examples for mixpanel, delighted, salesforce and wootric above
- DataProcessor : transfers and loads data between data components. [see examples](documentation/data_processor.ipynb)


### Adding ipython notebook files to github
Use git large file system (git lfs). See [documentation](https://git-lfs.github.com/?utm_source=github_site&utm_medium=jupyter_blog_link&utm_campaign=gitlfs)

- if using mac install git-lfs using brew ```brew install git-lfs```
- install lfs ```git lfs install```
- track ipynb files in your project. go to the project folder and do ```git lfs track "*.ipynb"```
- add ```.*ipynb_checkpoints/``` to .gitignore file
- Finally add .gitattributes file ```git add .gitattributes```


### hosting python package in pypi
- add your credentials to $HOME/.pypirc file [see instructions](https://packaging.python.org/guides/migrating-to-pypi-org/#uploading)
- build the code : ```python setup.py build && python setup.py clean && python setup.py install```
- push to pypitest : ```python setup.py sdist upload -r pypitest```
- push to pypi prod : ```python setup.py sdist upload -r pypi```
