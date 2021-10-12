## Integrate s3 functionality into a spark_session  
  
Spire data are stored mostly in parquet or delta format on s3 (excluding model artifacts on MLFlow and Workflow artifacts on postgres). While it is possible to set up a boto3 / aws CLI integration to read data from s3 locally, in most cases the integration is assumed to exist in the spark session itself. Therefore, in order to properly test the codebase, it is necessary to make a similar s3 integration locally.   
  
For instance, if I wanted to test the functionality in `scoring.py`, a core part of this process is `load_features`. In some cases it may be sufficient to write tests with a fixture or mocking out this functionality, but in other cases where we want to examine the integration or the actual data, subverting this functionality with some other local process is not an ideal substitute.  
  
**NOTE(Max):** As of writing, this functionality is still somewhat in flux and the instructions I provide might not 100% apply, but this should set you in the right direction.  

## Spark  
  
Databricks 9.x uses Spark 3.1.2 with Hadoop 2.7. It is not possible to brew install this build of Spark nor pip intall this build of pyspark. Instead, while it is still necessary to have the pyspark library, the version of Spark used by Spire should be downloaded off of the Apache Spark website, where it is possible to download this build.  
  
In bash_profile or exported into the environment via other means, SPARK_HOME should be set to this path e.g. `export SPARK_HOME=/usr/local/lib/spark-3.1.2-bin-hadoop2.7/` and $SPARK_HOME/bin should be added to PATH e.g. `export PATH=$PATH:$SPARK_HOME/bin`  
  
Note that, in fact the SPARK_HOME set in the docker image (and by extension the deployments) is the pip installed pyspark 3.1.2 which uses Hadoop 3.2.0, and that this does appear to work with Databricks 9.x despite that using Hadoop 2.7. I believe this is because the Spark code run by Spire would all happen on the databricks clusters, and the Databricks image uses the correct build of Hadoop.  
  
## Delta  
  
Delta is a Databricks library for a file format that builds off of parquet, which we use for some of our data. In order to read or write Delta files with pyspark locally / not on a Databricks cluster, the `configure_spark_with_delta_pip` function wraps around the spark_session builder, as seen in the `init_spark` function in `spire.utils.spark_helpers` script.  
  
## Configs  
  
The `$SPARK_HOME/conf` directory should exist, or else needs to be created, as does the `spark-defaults.conf` file in that directory.  
  
The spark-defaults.conf file should have the following information for accessing the aws credentials for s3:  

```
spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.profile.ProfileCredentialsProvider,com.amazonaws.auth.profile.ProfileCredentialsProvider,org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider
```  
  
## Jars  
  
It may be necessary to manually add the following jars to the $SPARK_HOME/jars directory. The specific versions may change depending on your version of Spark, Hadoop, or Java, and some digging on Maven or another jar repository may be necessary.  
  
Given the aforementioned `configure_spark_with_delta_pip` some or all of this may no longer be necessary, there is still some trial and error since upgrading Spire's Databricks Runtime to 9.x and additional changes.  
  
### Hadoop AWS Jar  
curl -sSL https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar > ${SPARK_HOME}/jars/hadoop-aws-3.2.0.jar  
  
### AWS Java SDK Bundle Jar (NOTE: May require aws-java-bundle instead of aws-java-sdk-bundle depending on version)  
curl -sSL https://search.maven.org/remotecontent?filepath=com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar > ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.11.375.jar  
  
### Delta-Core Jar
curl -sSL https://repo1.maven.org/maven2/io/delta/delta-core_2.12/1.0.0/delta-core_2.12-1.0.0.jar > $SPARK_HOME/jars/delta-core_2.12-1.0.0.jar  
  