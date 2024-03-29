## Week 5 Overview

* [DE Zoomcamp 5.1.1 - Introduction to Batch processing](#de-zoomcamp-511---introduction-to-batch-processing)
* [DE Zoomcamp 5.1.2 - Introduction to Spark](#de-zoomcamp-512---introduction-to-spark)
* [DE Zoomcamp 5.3.1 - First Look at Spark/PySpark](#de-zoomcamp-531---first-look-at-sparkpyspark)
* [DE Zoomcamp 5.3.2 - Spark DataFrames](#de-zoomcamp-532---spark-dataframes)
* [DE Zoomcamp 5.3.3 - (Optional) Preparing Yellow and Green Taxi Data](./05_taxi_schema.ipynb)
* [DE Zoomcamp 5.3.4 - SQL with Spark](./06_spark_sql.ipynb)
* [DE Zoomcamp 5.3.5 - Spark with cloud](#de-zoomcamp-534---spark-with-cloud)
* [DE Zoomcamp 5.3.6 - Creating a Local Spark Cluster](#creating-a-local-spark-cluster)

## [DE Zoomcamp 5.1.1 - Introduction to Batch processing](https://www.youtube.com/watch?v=dcHe5Fl3MF8&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)

Batch jobs are routines that are run in regular intervals. The most common types of batch jobs are either daily or hourly jobs. Batch jobs process data after the reference time period is over (e.g., after a day ends, a batch job processes all data that was gathered in that day). Batch jobs are easy to manage, retry (which also helps for fault tolerance) and scale. The main disadvantage is that we won't have the most recent data available, since we need to wait for an interval to end and our batch workflows run before being able to do anything with such data.

## [DE Zoomcamp 5.1.2 - Introduction to Spark](https://www.youtube.com/watch?v=FhaqbEOuQ8U&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)

Apache Spark is a data processing engine (i.e., the processing happens _in_ Spark) that is normally used for batch processing (although it can also be used for streaming).

Spark typically pulls data from a data lake, performs some processing steps and stores the data back in the data lake. Spark can be used for tasks where we would use SQL. However, the engine is recommended for dealing directly with files and in situations where we need more flexibility than SQL offers, such as: if we want to split our code into different modules, write unit tests or even some functionality that may not be possible to write using SQL (e.g., machine learning-related routines, such as training and/or using a model).

In the lesson, Alexey Grigorev gives us the following advice: _"If you can express something with SQL, you should go with SQL. But for cases where you cannot you should go with Spark"_.

## [DE Zoomcamp 5.3.1 - First Look at Spark/PySpark](https://www.youtube.com/watch?v=r_Sf6fCB40c&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)

## Windows

Here we'll show you how to install Spark 3.3.2 for Windows.
We tested it on Windows 10 and 11 Home edition, but it should work
for other versions distros as well

In this tutorial, we'll use [MINGW](https://www.mingw-w64.org/)/[Gitbash](https://gitforwindows.org/) for command line

If you use WSL, follow the instructions from [linux.md](linux.md) 


### Installing Java

Spark needs Java 11. Download it from here: [https://www.oracle.com/de/java/technologies/javase/jdk11-archive-downloads.html](https://www.oracle.com/de/java/technologies/javase/jdk11-archive-downloads.html). Select “Windows x64 Compressed Archive” (you may have to create an oracle account for that)

Unpack it to a folder with no space in the path. We use `C:/tools` - so the full path to JDK is `/c/tools/jdk-11.0.13`


Now let’s configure it and add it to `PATH`:

```bash
export JAVA_HOME="/c/Program Files/Java/jdk-11.0.17"
export PATH="${JAVA_HOME}/bin:${PATH}"
```

Check that Java works correctly:

```bash
java --version
```

Output:

```
java 11.0.13 2021-10-19 LTS
Java(TM) SE Runtime Environment 18.9 (build 11.0.13+10-LTS-370)
Java HotSpot(TM) 64-Bit Server VM 18.9 (build 11.0.13+10-LTS-370, mixed mode)
```

### Hadoop

Next, we need to have Hadoop binaries. 

We'll need Hadoop 3.2 which we'll get from [here](https://github.com/cdarlint/winutils/tree/master/hadoop-3.2.0).

Create a folder (`/c/tools/hadoop-3.2.0`) and put the files there 

```bash
HADOOP_VERSION="3.2.0"
PREFIX="https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-${HADOOP_VERSION}/bin/"

FILES="hadoop.dll hadoop.exp hadoop.lib hadoop.pdb libwinutils.lib winutils.exe winutils.pdb"

for FILE in ${FILES}; do
  wget "${PREFIX}/${FILE}"
done
```

Add it to `PATH`:

```bash
export HADOOP_HOME="/c/tools/hadoop-3.2.0"
export PATH="${HADOOP_HOME}/bin:${PATH}"
```

### Spark

Now download Spark. Select version 3.3.2 

```bash
wget https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
```


Unpack it in some location without spaces, e.g. `c:/tools/`: 

```bash
tar xzfv spark-3.3.2-bin-hadoop3.tgz
```

Let's also add it to `PATH`:

```bash
export SPARK_HOME="/c/tools/spark-3.3.2-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"
```

### Testing it

Go to this directory

```bash
cd spark-3.3.2-bin-hadoop3
```

And run spark-shell:

```bash
./bin/spark-shell.cmd
```

At this point you may get a message from windows firewall — allow it.


There could be some warnings (like this):

```
WARNING: An illegal reflective access operation has occurred
WARNING: Illegal reflective access by org.apache.spark.unsafe.Platform (file:/C:/tools/spark-3.3.2-bin-hadoop3/jars/spark-unsafe_2.12-3.3.2.jar) to constructor java.nio.DirectByteBuffer(long,int)
WARNING: Please consider reporting this to the maintainers of org.apache.spark.unsafe.Platform
WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
WARNING: All illegal access operations will be denied in a future release
```

You can safely ignore them.

Now let's run this:

```
val data = 1 to 10000
val distData = sc.parallelize(data)
distData.filter(_ < 10).collect()
```

### PySpark

It's the same for all platforms. Go to [pyspark.md](pyspark.md). 

---
## One more instruction
To be able to use PySpark, I created a conda environment and installed it using the following commands:
```
conda install openjdk
conda install pyspark
```

**Step 1:** `SparkSession` is the entry point into all functionality in Spark. We usually create a `SparkSession` as in the following example:
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
```

We can analyze the code above in smaller parts:
* `builder` is a class attribute that is and instance of `pyspark.sql.session.SparkSession.Builder` that is used to construct `SparkSession` instances.
* `master("local[*]")` sets the Spark master URL to connect to. In this example, `local` refers to the local machine and `[*]` tells Spark to use all available cores (e.g., if we wanted to use only 2 cores, we would write `local[2]`).
* `appName('test')` sets the application name that is shown in Spark UI.
* `getOrCreate()` returns an existing `SparkSession`, if available, or creates a new one.

**Step 2:** read the fhvhv_tripdata_2021-01.csv.gz (available [here](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhvhv)).
```python
df = spark.read \
    .option("header", "true") \
    .option("inferSchema",True) \
    .csv('fhvhv_tripdata_2021-01.csv.gz')
df.show(5)
```

**Step 3:** check the inferred schema through `df.schema`. For the fhvhv tripdata, we get the following output:
```
StructType(
    List(
        StructField(hvfhs_license_num,StringType,true),
        StructField(dispatching_base_num,StringType,true),
        StructField(pickup_datetime,StringType,true),
        StructField(dropoff_datetime,StringType,true),
        StructField(PULocationID,IntegerType,true),
        StructField(DOLocationID,IntegerType,true),
        StructField(SR_Flag,IntegerType,true)
    )
)
```

It is possible to observe that `pickup_datetime` and `dropoff_datetime` were both interpreted as strings. Furthermore, if we haven't used `inferSchema`, Spark would interpret `PULocationID` and `DOLocationID` as LongTypes (which use 8 bytes instead of the 4 bytes used by IntegerType). When these things happen, we may want to achieve better performance and intepret all fields correctly. Then, we can specify the expected schema:
```python
from pyspark.sql import types
schema = types.StructType(
    [
        types.StructField('hvfhs_license_num', types.StringType(), True),
        types.StructField('dispatching_base_num', types.StringType(), True),
        types.StructField('pickup_datetime', types.TimestampType(), True),
        types.StructField('dropoff_datetime', types.TimestampType(), True),
        types.StructField('PULocationID', types.IntegerType(), True),
        types.StructField('DOLocationID', types.IntegerType(), True),
        types.StructField('SR_Flag', types.IntegerType(), True)
    ]
)

df = spark.read \
    .option("header", "true") \
    .option("inferSchema",True) \
    .csv('fhvhv_tripdata_2021-01.csv.gz')

df.schema
```

**Step 4:** save DataFrame as parquet. As explained by the instructor, it is not good to have a smaller number of files than CPUs (because only a subset of CPUs will be used and the remaining will be idle). For such, we first use the repartition method and then save the data as parquet. Suppose we have 8 cores, then we can repartition our dataset into 24 parts.
```
df = df.repartition(24)
df.write.parquet('fhvhv/2021/01/')
```

Then, if we list the output directory, we should get an output similar to this one:

    part-00000-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00001-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00002-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00003-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00004-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00005-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00006-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00007-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00008-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00009-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00010-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00011-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00012-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00013-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00014-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00015-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00016-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00017-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00018-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00019-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00020-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00021-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00022-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00023-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    _SUCCESS

## [DE Zoomcamp 5.3.2 - Spark DataFrames](https://www.youtube.com/watch?v=ti3aC1m3rE8&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)

We start by reading the parquet data that we saved in the end of the last lesson.
```python
df = spark.read.parquet('fhvhv/2021/01/')
```

We can print the schema, which is also contained in the files and is interpreted by Spark, using `df.printSchema()` and get the output below.

    root
    |-- hvfhs_license_num: string (nullable = true)
    |-- dispatching_base_num: string (nullable = true)
    |-- pickup_datetime: timestamp (nullable = true)
    |-- dropoff_datetime: timestamp (nullable = true)
    |-- PULocationID: integer (nullable = true)
    |-- DOLocationID: integer (nullable = true)
    |-- SR_Flag: integer (nullable = true)

Next, we can use the `select()` and `filter()` methods to execute simple queries on our DataFrame.
```python
df.select('pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID') \
    .filter(df.hvfhs_license_num == 'HV0003')
```
In the code above we selected 4 columns where `hvfhs_license_num` is equal to 'HV0003'.

### Actions vs. Transformations

In Spark, we have a distinction between Actions (code that is executed immediately) and Transformations (code that is lazy, i.e., not executed immediately).

Examples of Transformations are: selecting columns, data filtering, joins and groupby operations. In these cases, Spark creates a sequence of transformations that is executed only when we call some method like `show()`, which is an example of an Action.

Examples of Actions are: `show()`, `take()`, `head()`, `write()`, etc.

### Spark SQL Functions

Spark has many predefined SQL-like functions.
```python
from pyspark.sql import functions as F
df \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .withColumn('dropoff_date', F.to_date(df.dropoff_datetime)) \
    .select('pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID') \
    .show()
```

In addition, we can also define custom functions to run on our DataFrames.
```python
def crazy_stuff(base_num):
    num = int(base_num[1:])
    if num % 7 == 0:
        return f's/{num:03x}'
    elif num % 3 == 0:
        return f'a/{num:03x}'
    return f'e/{num:03x}'

crazy_stuff_udf = F.udf(crazy_stuff, returnType=types.StringType())

df \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .withColumn('dropoff_date', F.to_date(df.dropoff_datetime)) \
    .withColumn('base_id', crazy_stuff_udf(df.dispatching_base_num)) \
    .select('base_id', 'pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID') \
    .show()
```

## [DE Zoomcamp 5.3.4 - Spark with cloud](https://www.youtube.com/watch?v=Yyz293hBVcQ&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)

For transfer all date from compute to datalake, you can use this comand
```
gsutil cp -r pq/ gs://dtc_data_lake_datacamp-378414/pq
```

## Has anyone figured out how to read from GCP data lake instead of downloading all the taxi data again?
There’s a few extra steps to go into reading from GCS with PySpark

1.  IMPORTANT: Download the Cloud Storage connector for Hadoop here: https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage#clusters
As the name implies, this .jar file is what essentially connects PySpark with your GCS

2. Move the .jar file to your Spark file directory. I installed Spark using homebrew on my MacOS machine and I had to create a /jars directory under "/opt/homebrew/Cellar/apache-spark/3.2.1/ (where my spark dir is located)

3. In your Python script, there are a few extra classes you’ll have to import:
import pyspark
```python
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
```

4. You must set up your configurations before building your SparkSession. Here’s my code snippet:
```python
credentials_location = '/home/Дмитрий/.google/credentials/datacamp-378414-e59ad2993706.json'

# config for google cloud storage
conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", "/home/Дмитрий/datacamp/dataeng-zoomcamp/week_5_batch_processing/data/lib/gcs-connector-hadoop3-2.2.5.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)

sc = SparkContext(conf=conf)
sc._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
sc._jsc.hadoopConfiguration().set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
sc._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.json.keyfile", credentials_location)
sc._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.enable", "true")
```

5. Once you run that, build your SparkSession with the new parameters we’d just instantiated in the previous step:
```python
spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()
```

6. Finally, you’re able to read your files straight from GCS!
```python
df_green = spark.read.parquet("gs://{BUCKET}/green/202*/")
```

### [There is instruction how to use Spark like google cluster - Dataproc](https://www.youtube.com/watch?v=osAiAYahvh8&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
### [Connecting Spark to Big Query](https://www.youtube.com/watch?v=HIm2BOj8C0Q&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)

## [DE Zoomcamp 5.3.6 - Creating a Local Spark Cluster](https://www.youtube.com/watch?v=HXBwSlXo5IA&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb) 
1. Starting a Cluster Manually <br>
You can start a standalone master server by executing:
```bash
./sbin/start-master.sh
```
2. Similarly, you can start one or more workers and connect them to the master via:
```
./sbin/start-worker.sh <master-spark-URL>
```
3. Add opportunity parse variable from cli for a python script
Good example for this in file 4_spark_sql.py
4. Launch a python script with parameters
```bash
python 4_spark_sql.py \
        --input_green=/home/Дмитрий/datacamp/dataeng-zoomcamp/week_5_batch_processing/data/pq/green/2020/* \
        --input_yellow=/home/Дмитрий/datacamp/dataeng-zoomcamp/week_5_batch_processing/data/pq/yellow/2020/* \
        --output=/home/Дмитрий/datacamp/dataeng-zoomcamp/week_5_batch_processing/data/report/revenue2020/
```

Also you can use spark-submit for launch your jobs
```bash
URL="spark://sparl-datacamp.asia-east2-a.c.datacamp-378414.internal:7077"

spark-submit \
    --master="${URL}" \
    4_spark_sql.py \
    --input_green=/home/Дмитрий/datacamp/dataeng-zoomcamp/week_5_batch_processing/data/pq/green/2021/* \
    --input_yellow=/home/Дмитрий/datacamp/dataeng-zoomcamp/week_5_batch_processing/data/pq/yellow/2021/* \
    --output=/home/Дмитрий/datacamp/dataeng-zoomcamp/week_5_batch_processing/data/report/revenue2021/
```
5. When you finish work with claste you have to stop workes and master. <br>
Stops all worker instances on the machine the script is executed on.
``` bash
sbin/stop-worker.sh
```

Stops the master that was started via the sbin/start-master.sh script.
```
sbin/stop-master.sh
```
