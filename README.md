# RealtimeStreamBenchmark

## Data generator
**Module path:** StreamBench/generator.  
**Description:** This module defineds data generators for workloads. Parameters could be configured in configure files under resources folder.  
**Usage:**  
&nbsp;&nbsp;&nbsp;&nbsp;1. go to module foler;  
&nbsp;&nbsp;&nbsp;&nbsp;2. execute command "mvn clean package";  
&nbsp;&nbsp;&nbsp;&nbsp;3. get packaged jar file with dependencies -- generator-1.0-SNAPSHOT-jar-with-dependencies.jar;  
&nbsp;&nbsp;&nbsp;&nbsp;4. start generator: "java -cp generator*.jar fi.aalto.dmg.generator.GeneratorClass (interval)", interval is a parameter to control speed of generation;  

**Special case:** For workload KMeans, we should generate real centoirds in generator class `fi.aalto.dmg.generator.KMeansPoints`, then generate test data. Detail information is in the source of this class.

