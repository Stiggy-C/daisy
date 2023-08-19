### 2023-08-16
* MVEL integration.
  * Removed ExpressionService and replaced with [MvelExpressionService](src/main/java/io/openenterprise/daisy/spark/MvelExpressionService.java)
    as MVEL offer more flexibility over SpEL.
  * Added [AbstractMvelDatasetComponentImpl](src/main/java/io/openenterprise/daisy/spark/sql/AbstractMvelDatasetComponentImpl.java),
    [AbstractMvelPlotGeneratingDatasetComponentImpl](src/main/java/io/openenterprise/daisy/spark/sql/AbstractMvelPlotGeneratingDatasetComponentImpl.java)
    and [AbstractMvelStreamingDatasetComponentImpl](src/main/java/io/openenterprise/daisy/spark/sql/AbstractMvelStreamingDatasetComponentImpl.java)
    which makes use of [MvelExpressionService](src/main/java/io/openenterprise/daisy/spark/MvelExpressionService.java)
    * See [RecentPurchaseExampleMvelDatasetComponent](src/test/java/io/openenterprise/daisy/examples/RecentPurchaseExampleMvelDatasetComponent.java) and
      [RecentPurchaseExampleMvelDatasetComponentTest.java](src/test/java/io/openenterprise/daisy/examples/RecentPurchaseExampleMvelDatasetComponentTest.java) for usage

### 2023-07-29
* plotly-scala integration.
    * Added [AbstractPlotGeneratingDatasetComponentImpl](src/main/java/io/openenterprise/daisy/spark/sql/AbstractPlotGeneratingDatasetComponentImpl.java)
      which allows plotting of the resulting Dataset
        * [RecentPurchaseExampleDatasetComponent](src/test/java/io/openenterprise/daisy/examples/RecentPurchaseExampleDatasetComponent.java) has
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
  io.openenterprise.daisy.spark.sql.AbstractBaseDatasetComponentImpl.buildDataset(java.util.Map<java.lang.String,?>, io.openenterprise.daisy.spark.sql.CreateTableOrViewPreference).
* DeltaLake integration.