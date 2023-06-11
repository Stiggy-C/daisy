# Daisy
## What is Daisy?
Daisy stands for **D**ata **A**ggregation & **I**ntellectualizing **Sy**stem. It is
a simple (low code) data lake-house (wanna be) which built around Apache Spark & Spring Boot.

## Updates
### 2023-06-11
* Ability to create (both internal & external) table & (both global & local) view within Apache Spark with 
io.openenterprise.daisy.spark.sql.AbstractSparkSqlService.buildDataset(java.util.Map<java.lang.String,?>, io.openenterprise.daisy.spark.sql.CreateTableOrViewPreference).
* DeltaLake integration.

## What can Daisy offer?
* #### Data pipeline & streaming data pipeline
  * (streaming) data pipeline(s) can be implemented with little amount of code by anyone who has
  Spark experience.
* #### Machine Learning as a service
  * spark-mllib model can be built and prediction can be retrieved with little amount of code by anyone who 
  has Spark experience.
  * RESTful APIs to invoke model training & getting predictions on the fly.

## Design goals
* #### Low code
  * Data engineers with Apache Spark experience just need to fill in the blanks
* #### Mainstream frameworks & libraries
  * Can easily be tamed by anyone who has worked with Apache Spark & Spring Boot. 
* #### Minimalistic design
  * Keeping it simple & stupid and does not require a rocket scientist to work with Daisy

## Core components
In the lowest level, Daisy provide 4 core components for developers/engineers who have exposures to Apache Spark.

### AbstractDatasetService
This class represent building an aggregated dataset on an Apache Spark cluster. Furthermore, it provides logic to run as
pipeline providing that writeDataset is implemented. Spark engineers just need to extends this class and fill in the 
following methods, 

* Dataset<Row> buildDataset(Map<String, ?> parameters)
* void writeDataset(Dataset<Row> dataset, Map<String, ?> parameters) [if want to form a pipeline]

The implementation of this class is ideal to be used to form a pipeline in a batch job.

[RecentPurchaseExampleDatasetService](src/main/java/io/openenterprise/daisy/examples/RecentPurchaseExampleDatasetService.java)
is an example implementation of this class. It will read the transactions in a CSV files which is being stored in a S3 
bucket to be aggregated with the membership table in a MySQL database and write the results into a PostgreSQL database.

Usage of [RecentPurchaseExampleDatasetService.java](src/main/java/io/openenterprise/daisy/examples/RecentPurchaseExampleDatasetService.java) 
can be seen from [RecentPurchaseExampleDatasetServiceTest](src/test/java/io/openenterprise/daisy/examples/RecentPurchaseExampleDatasetServiceTest.java)

### AbstractStreamingDatasetService
This class represent building an aggregated streaming dataset on an Apache Spark cluster. Furthermore, it provides logic
to run as a streaming pipeline providing that writeDataset is implemented. Spark engineers just need to extends this class and fill in the
following methods,

* Dataset<Row> buildDataset(Map<String, ?> parameters)
* void writeDataset(Dataset<Row> dataset, Map<String, ?> parameters) [if want to form a streaming pipeline]

The implementation of this class is ideal for continuous data integration.

[RecentPurchaseExampleStreamingDatasetService.java](src/main/java/io/openenterprise/daisy/examples/RecentPurchaseExampleStreamingDatasetService.java)
is an example implementation of this class. It is the streaming version of 
[RecentPurchaseExampleDatasetService](src/main/java/io/openenterprise/daisy/examples/RecentPurchaseExampleDatasetService.java)

Usage of [RecentPurchaseExampleStreamingDatasetService](src/main/java/io/openenterprise/daisy/examples/RecentPurchaseExampleStreamingDatasetService.java)
can be seen from [RecentPurchaseExampleStreamingDatasetServiceTest](src/test/java/io/openenterprise/daisy/examples/RecentPurchaseExampleStreamingDatasetServiceTest.java)

### AbstractMachineLearningService
This class represents the foundation of a machine learning operation running on an Apache Spark cluster. Spark engineers
just need to extends this class and fill in the following methods to build/train machine learning model & use the 
built/trained model to get a prediction,

* Dataset<Row> buildDataset(Map<String, ?> parameters)
* M buildModel(Dataset<Row> dataset, Map<String, ?> parameters)
* Dataset<Row> predict(M model, String jsonString)

[ClusterAnalysisOnRecentPurchaseExample](src/main/java/io/openenterprise/daisy/examples/ml/ClusterAnalysisOnRecentPurchaseExample.java)
for an example implementation of this class. It will read the transactions in a CSV files which is being stored in a S3 
bucket to be aggregated with the membership table in a MySQL database. Aggregated data will be massaged to be used to 
build a ML model to get prediction.

Usage of [ClusterAnalysisOnRecentPurchaseExample](src/test/java/io/openenterprise/daisy/examples/ml/ClusterAnalysisOnRecentPurchaseExample.java)
can be seen from [ClusterAnalysisOnRecentPurchaseExampleTest](src/test/java/io/openenterprise/daisy/examples/ml/ClusterAnalysisOnRecentPurchaseExampleTest.java)

### AbstractPmmlBasedMachineLearning
**P**redictive **M**odel **M**arkup **L**anguage (PMML) is an XML-based predictive model interchange format. It allows
certain models build by tools like [scikit-learn](https://scikit-learn.org/stable/), a famous Python ML framework, to be
imported and run on Apache Spark/Daisy. This class provides method to import the PMML file from file or internet or from
S3. Imported PMML file will be converted to Spark model. Such model can be used by the predict method of this class.

Spark engineers do not have to do anything special, he/she just need to extends 
this class and give it an unique (Spring) bean name when necessary.

## RESTful APIs
### MlApiImpl

[MlApiImpl](src/main/java/io/openenterprise/daisy/spark/sql/rs/MlApiImpl.java) provides the following endpoints:

#### getPrediction
POST (http|https)://$host:$port/services/ml/{beanName}/predict?modelId=$modelId

#### trainModel
POST (http|https)://$host:$port/services/ml/{beanName}/train

### PipelineApiImpl

[PipelineApiImpl](src/main/java/io/openenterprise/daisy/spark/sql/rs/PipelinesApiImpl.java) provides the following endpoints:

#### triggerPipeline
POST (http|https)://$host:$port/services/pipelines/{beanName}/trigger

Click [here](src/main/resources/openapi.yaml) for the OpenAPI 3 definition for all the endpoints.

## Caveats
* Currently, Daisy is engineered to be run in Spark client mode. In another words, Daisy need to be hosted on its own,
be it the Docker engine/Kubernetes/physical server/VM.

* Due to the fact that Daisy right now can only run in Spark client mode. It can only be run in a single node for most 
of its operations. This may be an issue if want to deploy Daisy to a production environment. Such concern may not be as
significant if Daisy is being deployed to something like ECS as the ECS engine will try to start a new instance. The 
followings operations may not be limited by this constraint.

```markdown
1. Import a pre-build Spark ML model from another Daisy instance for prediction.
2. Import a PMML file from remote for prediction.
```

