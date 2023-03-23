 # HelloFresh Data Engineering Test Solution

Hellofresh recipe etl app process recipe data and generates reports for specific ingredients.

- Preprocess and stores recipe json to optimized parquet format
- Generates CSV report for average cook time by difficulty level for beef ingredient

## Solution Features
- ETL app is well structured for easy maintainability, extensibility, readability 
- etl app is configuration driven
- Easy to test and debug the application
- Modularized and packaged for easily dependency managment
- Can be deployed and run in local mode and spark cluster 

## Tech
- Spark and Python - for data processing

## ETL Application Structure

```bash
├───data            		    ---->  Contains input ,processed and output dataset
│   ├───input
│   ├───output
│   └───preprocess
├───configs/				    ---->  Contains config files for etl jobs
│       hf_recipe_ingred_report_etl_config.json
│       hf_recipe_preprocess_etl_config.json
├───common_utils 			    ---->  common modules for spark etl jobs
│   │   logging.py
│   │   spark.py
│   │   trans_udf.py
│   │   __init__.py
├───jobs					    ---->  Main ETL scripts
│       hf_recipe_ingred_report_etl.py
│       hf_recipe_preprocess_etl.py
│   build.sh				    ----> build script for cluster deployment
│   packages.zip			    ----> dependency packages for spark-submit
│   Pipfile
│   Pipfile.lock
```
# Application Functionality Overview

###  ETL Script :  hf_recipe_preprocess_etl.py
- Main drvier ETL script for pre process source which contain following functions
   - main () : Initialse spark,logger and config objects.Runs ETL pipeline process
   - read_data() : reads json from input folder and create spark dataframe
   - transform_data(): convert source dataframe with iso timeduration format to standard   format
   - write_data():wries transformed data to preprocess folder
  
### ETL Script :  hf_recipe_ingred_report_etl.py
- Main driver ETL script for generating ingredient report which contain functions for etl calculations. 
   - main () : Initialse spark,logger and config objects.Runs ETL pipeline process
   - read_data : reads preprocess parquet data and create spark dataframe
   - transform_data: filters beef ingredient,calculate average cook time(prep time + cook time) based difficulty level (easy < 30 , medium 30 to 90, hard for > 90)
   - write_data:writes calculated data to output/data/ folder
 
#  Common Modules
- spark.py : Generates spark session, spark logger and set config values
- logging.py : contains helper class for logging methods
- trans_udf.py : contains udf methods for data tranformation

## How to run solution locally or docker
- Download and extract spark-3.1.2-bin-hadoop2.7.tgz
- SET enviroment vairables JAVA_HOME, SPARK_HOME, HADOOP_HOME and include in PATH variables
- Clone the repo locally 
```sh
git clone https://github.com/hellofreshdevtests/rragavn-data-engineering-test.git
```

- verify the input and output path in json config files in config folder
- Run the spark- submit command from root folder of the project
```sh
spark-submit \
	--master local[*] \
	--py-files packages.zip \
	--files configs/hf_recipe_preprocess_etl_config.json \
	jobs/hf_recipe_preprocess_etl.py
	
spark-submit \
	--master local[*] \
	--py-files packages.zip \
	--files configs/hf_recipe_ingred_report_etl_config.json \
	jobs/hf_recipe_ingred_report_etl.py
```

- Solution can be run in docker using the same appraoch
- Solution can be run in spark cluster by passing spark cluster address parameters

## Output data
- please check the output csv generated in this path "data/output/"

## How to run unit test
- Run "pipenv install -dev" to setup project dependency in your local.
- Running unit test case require pyspark module
```sh
python -m unittest tests/test_hf_recipe_preprocess_etl.py
python -m unittest tests/test_hf_recipe_ingred_report_etl.py
```

## Config Managment

- Any static and dynamic parameters required by ETL pipeline jobs are stored in JSON format in configs/*etl_config.json
- ETL job configuration can be explicitly version controlled within the same project structure
- config parameters are submitted to spark-submit utility and sent to spark cluster for execution

## Data Quality
- Data Quality checks can be implemented using pydeequ for automated generation of data quality metrics and ensure data quality constraints

## Scheduling

- Spark jobs can be scheduled using airflow dags

## CI/CD
- Dockerfile — provisions the dockerized container to run jenkins agent service
- Makefile — This Makefile utility zips all the code, dependencies, and config in the 	    packages.zip file so that Jenkins can create the artifact and publish to artifact repo
Jenkinsfile — Runs the CI/CD process, prepares env, run build, test and deploy the artifacts using Makefile utilitu

## Debug & Performance Tuning
- Use the Spark UI to understand and debug the performance of ETL job, UI provides jobs,stages, tasks and executor details
- Cluster Properties tuniing : Set the right parameters for the spark job: number of executors, memory allocation, driver memory
- Data management : Use of right partitions, Cache the intermediate results or repeated operations for optimized performance


## Reference
- abtractions and best practices are followed and implemented based on below resource
   pydata: <https://github.com/pchrabka/PySpark-PyData>

