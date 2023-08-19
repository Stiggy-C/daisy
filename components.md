### [AbstractBaseDatasetComponentImpl](src/main/java/io/openenterprise/daisy/spark/sql/AbstractBaseDatasetComponentImpl.java)
This class is the base of other dataset related classes in Daisy. Necessary methods to build (Spark) dataset and to
create a (Spark) table/view are implemented in this class. It also defines the (abstract) methods which need to be
implemented when extending this class.

The following methods are implemented in this class,

* buildDataset(java.util.Map<java.lang.String,?>, io.openenterprise.daisy.spark.sql.CreateTableOrViewPreference)
* createTable
* createView

The following abstract methods are defined in this class and must be implemented when extending this class.

* buildDataset(java.util.Map<java.lang.String,?>) (Custom logic to build the desired dataset from data sources goes here.)

### [AbstractDatasetComponentImpl](src/main/java/io/openenterprise/daisy/spark/sql/AbstractDatasetComponentImpl.java)
This class add on top of AbstractBaseDatasetComponentImpl to provide the ability to write the built dataset to desired
data sink. It also provides methods to run as a data pipeline. The following methods need to be implemented when
extending this class,

* buildDataset(java.util.Map<java.lang.String,?>)
* pipeline(java.util.Map<java.lang.String,?>)
* pipeline(java.util.Map<java.lang.String,?>, io.openenterprise.daisy.spark.sql.CreateTableOrViewPreference)
* writeDataset(org.apache.spark.sql.Dataset<Row>, java.util.Map<java.lang.String,?>)

[RecentPurchaseExampleDatasetComponent](src/test/java/io/openenterprise/daisy/examples/RecentPurchaseExampleDatasetComponent.java)
is an example implementation of this class. It reads the transactions in a CSV files stored in a S3 bucket to be aggregated
with the membership table in a MySQL database and write the results into a PostgreSQL database.

Usage of this class can be seen from [RecentPurchaseExampleDatasetComponentTest](src/test/java/io/openenterprise/daisy/examples/RecentPurchaseExampleDatasetComponentTest.java)

### [AbstractMvelDatasetComponentImpl](src/main/java/io/openenterprise/daisy/spark/sql/AbstractMvelDatasetComponentImpl.java)
allows MVEL based expressions to be injected (by Spring Boot) to build dataset, write dataset or run as pipeline

### [AbstractMvelPlotGeneratingDatasetComponentImpl](src/main/java/io/openenterprise/daisy/spark/sql/AbstractMvelPlotGeneratingDatasetComponentImpl.java)
is an extension of [AbstractMvelDatasetComponentImpl](src/main/java/io/openenterprise/daisy/spark/sql/AbstractMvelDatasetComponentImpl.java) which add
the ability to generate plot powered by Plotly or JSON to be used with Plotly by the injected MVEL based expressions.

[RecentPurchaseExampleMvelDatasetComponent](src/test/java/io/openenterprise/daisy/examples/RecentPurchaseExampleMvelDatasetComponent.java) is an 
example of this. Expressions in [recentPurchaseExampleMvelPipeline.yaml](src/test/resources/recentPurchaseExampleMvelPipeline.yaml) are injected
and make used to build dataset, build JSON for Plotly, build Plotly plot, write dataset and even run as a pipeline.

### [AbstractMvelStreamingDatasetComponentImpl](src/main/java/io/openenterprise/daisy/spark/sql/AbstractStreamingDatasetComponentImpl.java) allows
MVEL expressions to be injected by Spring Boot to build (streaming) dataset, stream dataset to sink and even run as a pipeline.

### [AbstractPlotGeneratingDatasetComponentImpl](src/main/java/io/openenterprise/daisy/spark/sql/AbstractPlotGeneratingDatasetComponentImpl.java)
Extending AbstractDatasetComponentImpl, this class add integration with plotly-scala to generate plotly JS powered plot in
a html file or generate the JSON to be used by plotly JS. The following methods need to be implemented when extending
this class,

* getPlotData(org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>, java.util.Map<java.lang.String,java.lang.Object>)
* getPlotSetting(java.util.Map<java.lang.String,java.lang.Object>)
* <PD, PS extends io.openenterprise.daisy.PlotSettings>plot(java.lang.String path, PD, PS)
* savePlotToCloudStorage(java.net.URI, java.io.File)
* <PD, PS extends io.openenterprise.daisy.PlotSettings>toPlotJson(PD, PS)

[RecentPurchaseExampleDatasetComponent](src/test/java/io/openenterprise/daisy/examples/RecentPurchaseExampleDatasetComponent.java)
is an example implementation of this class.

### [AbstractStreamingDatasetComponentImpl](src/main/java/io/openenterprise/daisy/spark/sql/AbstractStreamingDatasetComponentImpl.java)
This class is the streaming counter part of [AbstractDatasetComponentImpl](src/main/java/io/openenterprise/daisy/spark/sql/AbstractDatasetComponentImpl.java).
It has methods to read & write from/to streaming data sources/sink and methods to run as a streaming data pipeline. The
following methods need to be implemented when extending this class,

* buildDataset(java.util.Map<java.lang.String,?>)
* streamingPipeline(java.util.Map<java.lang.String,?>)
* streamingPipeline(java.util.Map<java.lang.String,?>, io.openenterprise.daisy.spark.sql.CreateTableOrViewPreference)
* writeDataset(org.apache.spark.sql.Dataset<Row>, java.util.Map<java.lang.String,?>)

[RecentPurchaseExampleStreamingDatasetComponent.java](src/test/java/io/openenterprise/daisy/examples/RecentPurchaseExampleStreamingDatasetComponent.java)
is an example implementation of this class. It is the streaming version of
[RecentPurchaseExampleDatasetComponent](src/test/java/io/openenterprise/daisy/examples/RecentPurchaseExampleDatasetComponent.java)

Usage of this class can be seen from
[RecentPurchaseExampleStreamingDatasetComponentTest](src/test/java/io/openenterprise/daisy/examples/RecentPurchaseExampleStreamingDatasetComponentTest.java)

### [AbstractMachineLearningServiceImpl](src/main/java/io/openenterprise/daisy/spark/ml/AbstractMachineLearningComponentImpl.java)
This class contains the foundation of machine learning workflow running on an Apache Spark cluster. Spark engineers
just need to extend this class and fill in the following methods to build/train machine learning model, store the built
model to both cloud & local storage and use the built/trained model to get a prediction,

* buildDataset(java.util.Map<java.lang.String,?>)
* buildModel(org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>, java.util.Map<java.lang.String,java.lang.Object>, io.openenterprise.daisy.spark.ml.ModelStorage)
* buildModel(org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>, java.util.Map<java.lang.String,?>)
* predict(String, String, Map<String, ?>, ModelStorage)

[ClusterAnalysisOnRecentPurchaseExample](src/test/java/io/openenterprise/daisy/examples/ml/HongKongMark6LotteryResultClusterAnalysis.java)
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