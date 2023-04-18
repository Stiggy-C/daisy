# Daisy
## What is Daisy?
Daisy stands for **D**ata **A**ggregation & **I**ntellectualizing **Sy**stem. It is
a simple data lakehouse (wanna be) which built around Apache Spark & Spring Boot.

## Design goals
* #### Low code
  * Data engineers with Apache Spark experience just need to fill in the blank
* #### Mainstream frameworks & libraries
  * Can easily be tamed by anyone who worked with Apache Spark & Spring Boot. 
* #### Minimalist design
  * Keeping it simple & stupid and does not require rocket scientists to work with Daisy

## Core components
In the lowest level, Daisy provide 3 core components for Spark engineers.

### AbstractPipeline
This class represents the foundation of a data pipeline running on an Apache Spark cluster. Spark engineers just need to
extends this class and fill in the necessary logic to complete a data pipeline. The implementation of this class is 
ideal to be used in a batch job.

One can take a look at ```io.openenterprise.daisy.examples.RecentPurchaseExamplePipeline``` for an example 
implementation of this class.

### AbstractStreamingPipeline
This class represents the foundation of a streaming pipeline running on an Apache Spark cluster. Spark engineers just 
need to extends this class and fill in the necessary logic to complete a streaming data pipeline. The implementation of 
this class is ideal for continuous data integration.

One can take a look at ```io.openenterprise.daisy.examples.RecentPurchaseExampleStreamingPipeline``` for an example
implementation of this class.

### AbstractMachineLearning
This class represents the foundation of a machine learning operation running on an Apache Spark cluster.  Spark engineers just
need to extends this class and fill in the necessary logic to build/train machine learning model & use the built/trained
model to get a prediction.

One can take a look at ```io.openenterprise.daisy.examples.ml.ClusterAnalysisOnRecentPurchaseExample``` for an example
implementation of this class.

## Caveats
Currently, Daisy is engineered to be run in Spark client mode. In another words, Daisy need to be hosted on its own, be 
it the Docker engine/Kubernetes/physical server/VM.