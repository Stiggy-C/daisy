# Daisy
## What is Daisy?
Daisy stands for **D**ata **A**ggregation & **I**ntellectualizing **Sy**stem. It is
a simple data lakehouse (wanna be) which built around Apache Spark & Spring Boot.

## What can Daisy offer?
* #### Data pipeline & streaming data pipeline
  * Backed by Apache Spark, (streaming) data pipeline(s) can be implemented with little amount of code by anyone who has
  Spark experience.
* #### Machine Learning as a service
  * Backed by Apache Spark, model can be built and prediction can be retrieved with little amount of code by anyone who 
  has Spark experience & implemented logic can be accessed by 3rd parties via RESTful API.

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
This class represents the foundation of a machine learning operation running on an Apache Spark cluster. Spark engineers
just need to extends this class and fill in the following methods to build/train machine learning model & use the 
built/trained model to get a prediction,

* Dataset<Row> buildDataset(Map<String, ?> parameters)
* M buildModel(Dataset<Row> dataset, Map<String, ?> parameters)
* Dataset<Row> predict(M model, String jsonString)

One can take a look at [ClusterAnalysisOnRecentPurchaseExample](src/main/java/io/openenterprise/daisy/examples/ml/ClusterAnalysisOnRecentPurchaseExample.java)
for an example implementation of this class.

### AbstractPmmlBasedMachineLearning
**P**redictive **M**odel **M**arkup **L**anguage (PMML) is an XML-based predictive model interchange format. It allows
certain models build by tools like [scikit-learn](https://scikit-learn.org/stable/), a famous Python ML framework, to be
imported and run on Apache Spark/Daisy. This class provides method to import the PMML file from file or internet or from
S3. Imported PMML file will be converted to Spark model. Such model can be used by the predict method of this class.

Spark engineers do not have to do anything special, he/she just need to extends 
this class and give it an unique bean name when necessary.

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

