### 2023-07-29
* plotly-scala integration.
    * Added [AbstractPlotGeneratingDatasetServiceImpl](src/main/java/io/openenterprise/daisy/spark/sql/AbstractPlotGeneratingDatasetServiceImpl.java)
      which allows plotting of the resulting Dataset
        * [RecentPurchaseExampleDatasetService](io.openenterprise.daisy.examples.RecentPurchaseExampleDatasetService) has
          been updated to showcase the integration

### 2023-07-15
* Added [ExpressionService](src/main/java/io/openenterprise/daisy/spark/ExpressionService.java) to allow on the fly
  evaluation of SpEL expression against connected Spark cluster. Reference
  [ExpressionServiceTest](src/test/java/io/openenterprise/daisy/spark/sql/ExpressionServiceTest.java) for example usage.
* Added [SqlStatementService](src/main/java/io/openenterprise/daisy/spark/sql/SqlStatementService.java) to allow on the
  fly evaluation of Spark SQL statement against connected Spark cluster. Reference
  [SqlStatementServiceTest](src/test/java/io/openenterprise/daisy/spark/sql/SqlStatementServiceTest.java) for example usage.

### 2023-06-11
* Ability to create (both internal & external) table & (both global & local) view within Apache Spark with
  io.openenterprise.daisy.spark.sql.AbstractBaseDatasetServiceImpl.buildDataset(java.util.Map<java.lang.String,?>, io.openenterprise.daisy.spark.sql.CreateTableOrViewPreference).
* DeltaLake integration.