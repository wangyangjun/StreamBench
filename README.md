# RealtimeStreamBenchmark

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
