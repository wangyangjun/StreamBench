# RealtimeStreamBenchmark

## Experiment Environment Setup
**Module path:** script.  
**Description:** Setup experiment environment, install Storm, Flink, Spark, Kafka, HDFS and Zookeeper.  
**Usage:**  
&nbsp;&nbsp;&nbsp;&nbsp;1. Set ssh passwordless login experiment cluster;  
&nbsp;&nbsp;&nbsp;&nbsp;2. Update configureation file `cluster-config.json` to the role of each node in the cluster;  
&nbsp;&nbsp;&nbsp;&nbsp;3. Clone this repository to home directory of master node;  
&nbsp;&nbsp;&nbsp;&nbsp;4. Run `python scripts\pull-updates.py` to clone this repository to each node in the cluster;  
&nbsp;&nbsp;&nbsp;&nbsp;4. Run `python scripts\install.py` with different parameter to install softwares

## Data generator
**Module path:** StreamBench/generator.  
**Description:** This module defineds data generators for workloads. Parameters could be configured in configure files under resources folder.  
**Usage:**  
&nbsp;&nbsp;&nbsp;&nbsp;1. go to module foler;  
&nbsp;&nbsp;&nbsp;&nbsp;2. execute command `mvn clean package`;  
&nbsp;&nbsp;&nbsp;&nbsp;3. get packaged jar file with dependencies -- generator-1.0-SNAPSHOT-jar-with-dependencies.jar;  
&nbsp;&nbsp;&nbsp;&nbsp;4. start generator: `java -cp generator*.jar fi.aalto.dmg.generator.GeneratorClass (interval)`, interval is a parameter to control speed of generation;  

**Special case:** For workload KMeans, we should generate real centoirds in generator class `fi.aalto.dmg.generator.KMeansPoints`, then generate test data. Detail information is in the source of this class.

## Flink
**Module path:** StreamBench/flink.  
**Description:** This module implements APIs of core module with Flink's built-in APIs. Parameters could be configured in file `config.properties`.  
**Usage:**  
&nbsp;&nbsp;&nbsp;&nbsp;1. go to module foler;  
&nbsp;&nbsp;&nbsp;&nbsp;2. modify tage `program-class` in file `pom.xml` to specify the benchmark workload;  
&nbsp;&nbsp;&nbsp;&nbsp;3. execute command `mvn clean package`;  
&nbsp;&nbsp;&nbsp;&nbsp;3. get packaged jar file with dependencies -- flink-1.0-SNAPSHOT-jar-with-dependencies.jar;  
&nbsp;&nbsp;&nbsp;&nbsp;4. start flink cluster and submit workload job: `/usr/local/flink/bin/flink run flink-1.0-SNAPSHOT-jar-with-dependencies.jar`;  

**Log collection and statistic:**
1. collect log files: `python StreamBench/script/logs-collection.py flink flink.tar`   

