package io.openenterprise.daisy.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.Map;

/**
 * The base of all Apache Spark application.
 */
public abstract class AbstractSparkApplication {

    @Inject
    protected SparkSession sparkSession;

    /**
     * Built the aggregated {@link Dataset} from different data sources (say a RDBMS like Postgres).
     *
     * @param parameters
     * @return
     */
    @Nonnull
    public abstract Dataset<Row> buildDataset(@Nonnull Map<String, ?> parameters);
}
