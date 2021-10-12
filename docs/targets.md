# Spire Targets

## *Summary*

Spire Targets refers to the areas of the Spire code base used to extract, load, and transform labeled data.  
Spire Targets is a unified API to transform disparate data sources into a single schema, saved in a Delta Lake.  
Labeled Data, or targets, are extracted from the following data vendors:
- Acxiom
- Adobe
- CDS
- DFP
- Jumpshot [Retired]
- NCS
- Polk

Click here[link] for more information about each data vendor.
<br/><br/>
Production target data is stored in Delta format in S3 at `s3://cn-spire/targets`,  
with mirror tables stored in the development and staging buckets (`cn-spire-dev`, `cn-spire-staging`)  
The final schema targets are required to adhere to is:

```
StructType(
        [
            StructField("xid", StringType(), True),
            StructField("date", DateType(), True),
            StructField("vendor", StringType(), True),
            StructField("source", StringType(), True),
            StructField("group", StringType(), True),
            StructField("aspect", StringType(), True),
            StructField("value", FloatType(), True),
        ]
    )
```
`xid` field maps to a user's infinity id.
`date` referrs to the date the targets were written to the Delta lake.  
`vendor` should be one of the data vendors enumerated above.  
`source` refers to the data stream for a vendor (some vendors have more than one)  
`group` denotes the campaign or workflow for that row of data.  
`aspect` is used to store an extra metadata that may be important to save, but is not consistent through all vendors.  
`value` is the label for that row, and is usually a boolean value, but can also indicate a propensity level (1.0 - 10.0)  
<br/><br/>
<br/><br/>
## *Architecture*
<br/><br/>
Spire Targets operate using three components.  
- Spire Targets JobRunner classes
- Astronomer Airflow DAG
- Databricks Spark Job

The pattern followed by targets jobs is as follows:
A class containing the logic to extract, transform, and load the target data source needs to be added to  
`spire/framework/runners/targets`. This class must inherit from the base `TargetsRunner` class [here](https://github.com/CondeNast/spire/blob/main/spire/framework/runners/targets/targets_runner.py).
Once the Targets JobRunner exists in spire-core, a new DAG must be created for the job in spire-astronomer.  
The DAG must be supported by a schedule cron string and a cluster configuration in the configs.  
Once these configuration parameters have been added, the `TargetRunner` will execute a Spark job using a  
notebook or python script also specified in the Airflow configurations. Presently, notebooks at endpoint  
`Production/spire/targets/` contain the Databricks-side processing logic. Notebooks should contain another  
instantiation of the same `TargetsRunner` class that launched the job, but here the runner class will be  
executing processing logic for the relevant data source.

![Spire Targets Diagram](https://github.com/CondeNast/spire/blob/enhancement/add-docs/docs/spire-targets.svg)
<br/><br/>
<br/><br/>
## *Examples*


Airflow-side functionality:
```

# import a target runner class for the data vendor to process
from spire.framework.runners.targets import AdobeTargetsRunner

# instantiate the runner
# set the proper cluster configuration as an attribute on the runner 
  # more context here: https://github.com/CondeNast/spire-astronomer/blob/main/dags/helpers/loaders.py#L85
r = AdobeTargetsRunner(cluster_config=config["cluster_configuration"])

# extract run date from Airflow context arguments
run_date = context["execution_date"]

# launch the Spark job in Databricks
run_id = runner.run(run_date)
```

The Spark job launched by the Airflow task should look nearly uniform across each data vendor,  
as every TargetRunner uses the same interface to process data.


Databricks-side functionality:
```
from spire.framework.runners.targets import AdobeTargetsRunner

# parse the date argument launched from the Airflow task
date = datetime.datetime.strptime(dbutils.widgets.get('date'), '%Y-%m-%d').date() - datetime.timedelta(days=1)

# instantiate the runner
runner = AdobeTargetsRunner()

# execute `load_and_transform` -- this method will be called irrespective of the data vendor
targets = runner.load_and_transform(date)

# use the runner's `write_target` method to write the results of processing
 # as Delta in the Spire Targets Delta Lake at `s3://cn-spire/targets`
 
runner.write_target_df(targets, 'adobe', 'mcid', [date])

```
