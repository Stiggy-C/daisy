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
In the lowest level, Daisy provide 5 core components for developers/engineers who have exposures to Apache Spark.

### AbstractSparkSqlService
This class is the base of other dataset related classes in Daisy. Necessary methods to build (Spark) dataset and to 
create a (Spark) table/view are implemented in this class. It also defines the (abstract) methods which need to be 
implemented when extending this class.

The following methods are implemented in this class,

* buildDataset(java.util.Map<java.lang.String,?>, io.openenterprise.daisy.spark.sql.CreateTableOrViewPreference)
* createTable
* createView

The following abstract methods are defined in this class and must be implemented when extending this class.

* buildDataset(java.util.Map<java.lang.String,?>) (Custom logic to build the desired dataset from data sources goes here.)

### AbstractDatasetService
This class contains method about building an aggregated (Spark) dataset. Furthermore, it provides the ability to be run
as a data pipeline providing that the necessary method is implemented. Spark engineers just need to extends this class 
and fill in the following methods, 

* buildDataset(java.util.Map<java.lang.String,?>)
* pipeline(java.util.Map<java.lang.String,?>)
* pipeline(java.util.Map<java.lang.String,?>, io.openenterprise.daisy.spark.sql.CreateTableOrViewPreference)
* writeDataset

The implementation of this class is ideal to be used to build the dataset for testing & to build the dataset to create 
the (Spark) table/view for be used by others & to form the pipeline in a batch job.

[RecentPurchaseExampleDatasetService](src/main/java/io/openenterprise/daisy/examples/RecentPurchaseExampleDatasetService.java)
is an example implementation of this class. It will read the transactions in a CSV files which is being stored in a S3 
bucket to be aggregated with the membership table in a MySQL database and write the results into a PostgreSQL database.

Usage of [RecentPurchaseExampleDatasetService.java](src/main/java/io/openenterprise/daisy/examples/RecentPurchaseExampleDatasetService.java) 
can be seen from [RecentPurchaseExampleDatasetServiceTest](src/test/java/io/openenterprise/daisy/examples/RecentPurchaseExampleDatasetServiceTest.java)

### AbstractStreamingDatasetService
This class contains methods about building an aggregated (Spark) streaming dataset. Furthermore, it provides the ability 
to be run as a streaming pipeline providing that writeDataset is implemented. Spark engineers just need to extends 
this class and fill in the following methods,

* buildDataset(java.util.Map<java.lang.String,?>)
* streamingPipeline(java.util.Map<java.lang.String,?>)
* streamingPipeline(java.util.Map<java.lang.String,?>, io.openenterprise.daisy.spark.sql.CreateTableOrViewPreference)
* writeDataset

The implementation of this class is ideal to build a streaming pipeline for continuous data integration.

[RecentPurchaseExampleStreamingDatasetService.java](src/main/java/io/openenterprise/daisy/examples/RecentPurchaseExampleStreamingDatasetService.java)
is an example implementation of this class. It is the streaming version of 
[RecentPurchaseExampleDatasetService](src/main/java/io/openenterprise/daisy/examples/RecentPurchaseExampleDatasetService.java)

Usage of [RecentPurchaseExampleStreamingDatasetService](src/main/java/io/openenterprise/daisy/examples/RecentPurchaseExampleStreamingDatasetService.java)
can be seen from [RecentPurchaseExampleStreamingDatasetServiceTest](src/test/java/io/openenterprise/daisy/examples/RecentPurchaseExampleStreamingDatasetServiceTest.java)

### AbstractMachineLearningService
This class contains the foundation of machine learning workflow running on an Apache Spark cluster. Spark engineers
just need to extend this class and fill in the following methods to build/train machine learning model, store the built 
model to both cloud & local storage and use the built/trained model to get a prediction,

* buildDataset(java.util.Map<java.lang.String,?>)
* buildModel(org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>, java.util.Map<java.lang.String,java.lang.Object>, io.openenterprise.daisy.spark.ml.ModelStorage)
* buildModel(org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>, java.util.Map<java.lang.String,?>)
* predict

[ClusterAnalysisOnRecentPurchaseExample](src/main/java/io/openenterprise/daisy/examples/ml/ClusterAnalysisOnRecentPurchaseExample.java)
is an example implementation of this class. It will read (fake) sales transactions in a CSV files which is stored in a S3 
bucket to be aggregated with the membership table in a MySQL database. Aggregated data will be massaged to be used to 
build a ML model to get a prediction on user input.

Usage of [ClusterAnalysisOnRecentPurchaseExample](src/test/java/io/openenterprise/daisy/examples/ml/ClusterAnalysisOnRecentPurchaseExample.java)
can be seen from [ClusterAnalysisOnRecentPurchaseExampleTest](src/test/java/io/openenterprise/daisy/examples/ml/ClusterAnalysisOnRecentPurchaseExampleTest.java)

[HongKongMark6LotteryResultClusterAnalysis](src/test/java/io/openenterprise/daisy/examples/ml/HongKongMark6LotteryResultClusterAnalysis.java)
is another example implementation of this class. This example also makes use of [Delta Lake](https://delta.io/). It 
will store the dataset into the given S3 bucket in Delta Lake format to be used later during prediction.

Usage of [HongKongMark6LotteryResultClusterAnalysis](src/test/java/io/openenterprise/daisy/examples/ml/HongKongMark6LotteryResultClusterAnalysis.java)
can be seen from [HongKongMark6LotteryResultClusterAnalysisTest](src/test/java/io/openenterprise/daisy/examples/ml/HongKongMark6LotteryResultClusterAnalysisTest.java)

### AbstractPmmlBasedMachineLearning
**P**redictive **M**odel **M**arkup **L**anguage (PMML) is an XML-based predictive model interchange format. It allows
certain models build by tools like [scikit-learn](https://scikit-learn.org/stable/), a famous Python ML framework, to be
imported and run on Apache Spark/Daisy. This class provides method to import the PMML file from file or internet or from
S3. Imported PMML file will be converted to Spark model. Such model can be used by the predict method of this class.

Spark engineers do not have to do anything special, he/she just need to extends this class and give it an unique 
(Spring) bean name when necessary.

## RESTful APIs
Daisy provides the following APIs out of the box. The OpenAPI spec can be found [here](src/main/resources/openapi.yaml).

### DatasetApiImpl

[DatasetApiImpl](src/main/java/io/openenterprise/daisy/rs/DatasetApiImpl.java) provides the following endpoints:

#### buildDataset
POST (http|https:)//$host:$port/services/datasets/{beanName}?createTableOrViewPreference=$createTableOrViewPreference

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

