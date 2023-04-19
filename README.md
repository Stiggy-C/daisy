# Daisy
## What is Daisy?
Daisy stands for **D**ata **A**ggregation & **I**ntellectualizing **Sy**stem. It is
a simple data lakehouse (wanna be) which built around Apache Spark & Spring Boot.

## Design goals
* #### Low code
  * Data engineers with Apache Spark experience just need to fill in the blanks
* #### Mainstream frameworks & libraries
  * Can easily be tamed by anyone who has worked with Apache Spark & Spring Boot. 
* #### Minimalistic design
  * Keeping it simple & stupid and does not require rocket scientists to work with Daisy

## Core components
In the lowest level, Daisy provide 3 core components for Spark engineers.

### AbstractPipeline
This class represents the foundation of a data pipeline running on an Apache Spark cluster. Spark engineers just need to
extends this class and fill in the following methods to complete a streaming data pipeline, 

* Dataset<Row> buildDataset(Map<String, ?> parameters)
* void writeDataset(Dataset<Row> dataset, Map<String, ?> parameters)

The implementation of this class is ideal to be used in a batch job.

One can take a look at [RecentPurchaseExamplePipeline](src/main/java/io/openenterprise/daisy/examples/RecentPurchaseExamplePipeline.java) 
for an example implementation of this class.

### AbstractStreamingPipeline
This class represents the foundation of a streaming pipeline running on an Apache Spark cluster. Spark engineers just 
need to extends this class and fill in the following methods to complete a streaming data pipeline,

* Dataset<Row> buildDataset(Map<String, ?> parameters)
* void writeDataset(Dataset<Row> dataset, Map<String, ?> parameters)

The implementation of this class is ideal for continuous data integration.

One can take a look at [RecentPurchaseExampleStreamingPipeline](src/main/java/io/openenterprise/daisy/examples/RecentPurchaseExampleStreamingPipeline.java)
for an example implementation of this class.

### AbstractMachineLearning
This class represents the foundation of a machine learning operation running on an Apache Spark cluster.  Spark engineers
just need to extends this class and fill in the following methods to build/train machine learning model & use the 
built/trained model to get a prediction,

* Dataset<Row> buildDataset(Map<String, ?> parameters)
* M buildModel(Dataset<Row> dataset, Map<String, ?> parameters)
* Dataset<Row> predict(M model, String jsonString)

One can take a look at [ClusterAnalysisOnRecentPurchaseExample](src/main/java/io/openenterprise/daisy/examples/ml/ClusterAnalysisOnRecentPurchaseExample.java)
for an example implementation of this class.

## Caveats
* Currently, Daisy is engineered to be run in Spark client mode. In another words, Daisy need to be hosted on its own,
be it the Docker engine/Kubernetes/physical server/VM.
