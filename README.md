The goal of this python project is to build a bunch of wrappers that
can be reused for building data pipelines from -
i. standard database/messaging sources like postgres, mysql, greenplum, hive, mongo, kafka, redis, rabbitmq, kafka, etc. and
ii. cloud service providers like salesforce, mixpanel, jira, google-drive, delighted, wootric, etc


v1.0 of this package contains wrapper scripts for doing the following -

1. environment	        : Lets you source all the os environment variables

2. postgres_client      : Make connections to postgres / greenplum database using pyscopg2 or sqlalchemy
                          Use the connections to interact with database in interactive program or
                          run queries from a sql file using the connection

                          Lets you use psql and gpload utilities provided by EMC greenplum
                                https://gpdb.docs.pivotal.io/4350/common/client-docs-unix.html

3. mixpanel_client      : Connect to mixpanel api and fetch data using jql or export raw events data
                                https://mixpanel.com/help/reference/jql/api-reference
                                https://mixpanel.com/help/reference/data-export-api

4. mysql_client         : Lets you use mysql and other methods provided by PyMySQL Package

5. salesforce_client    : Create a connection to salesforce using simple_salesforce package
                            Git -  https://github.com/simple-salesforce/simple-salesforce

6. file_processor       : Create sftp connection using paramiko package. (FileTransfer Class)
                            Git - https://github.com/paramiko/paramiko.git
                          Other file manipulations like row_count, encryption, archive (File Class)

7. data_pipeline  		: This is the main class that's accessible to other projects. data_pipeline decides which
                            modules to call based on type of database (as defined in config file)
                            data_pipeline comprises of 3 classes
                            i) DataComponent : Each database connection is considered to be data-component object
                            ii) APICall : Each api call is an apicall object.
                            iii) DataProcessor : transfers and loads data between data components
                                The data pipeline consists of data from components and API.
                                data-processor can use individual data streams and process them

8. notification	 		: Send email notifications

9. mongo_client         : Load data to mongodb using bulk load. Run java script queries

10. redis_client        : Read data from a redis cache or load a redis cache

11. delighted_client    : Get nps scores and survey responses from delighted
                               https://delighted.com/docs/api/

12. wootric_client      : Gets nps scores and survey responses from wootric
                               http://docs.wootric.com/api

13. rabbitmq_system     : Currently has Publisher to publish messages in rabbitmq

14. kafka_system        : Currently allows Publisher and Consumer to use kafka in batch mode

15. dag_controller      : Functions needed to used sa_utility packages within airflow dag

See packaging, installation and examples below.


#------------------ Creating this python package using pip --------------------
cd <path to sa_utility>
pip install -e .

#------------------------------- Installation Steps ----------------------------

1) Download from github and build from scratch

git clone git@github.com:kartikra/dattasa.git

Check if the folder structure is as follows 

dattasa
    dattasa
        __init__.py
        environment.py
        postgres_client.py
        mysql_client.py
        salesforce_client.py
        mongo_client.py
        mixpanel_client.py
        file_processor.py
        data_pipeline.py
        notifications.py
        redis_client.py
        wootric_client.py
        delighted_client.py
        kafka_system.py
        rabbitmq_system.py
        dag_controller.py
    README.md
    setup.py
    setup.cfg

Check the version number specified in setup.py and change it if needed
Version should be >=  whatever is the current version installed for changes to take effect

cd dattasa
python setup.py build
python setup.py clean
python setup.py install

OR
#------------------ Creating this python package using pip --------------------
git clone git@github.com:kartikra/dattasa.git
cd dattasa
pip install -e .
pip install -U -e . (if upgrading)


#-- Environment Variables
- GPLOAD_HOME: Path to gpload package
- PROJECT_HOME: Path to python project directory 
- PROJECT_HOME/python_bash_scripts: python scripts to invoke gpload
- SQL_DIR: Place to keep all sql scripts
- TEMP_DIR: All temp files created in this folder
- LOG_DIR: All log files are created in this folder
