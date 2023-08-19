package io.openenterprise.daisy.spark.sql;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

/**
 * The base of a data pipeline to be run on an Apache Spark cluster.
 */
public abstract class AbstractDatasetComponentImpl extends AbstractBaseDatasetComponentImpl
        implements DatasetComponent {

    /**
     * Run this as a pipeline
     *
     * @param parameters
     * @throws AnalysisException
     */
    public void pipeline(@Nonnull Map<String, ?> parameters) throws AnalysisException {
        pipeline(parameters, null);
    }

    /**
     * Run this as a pipeline and create a temp view of the {@link Dataset} to be re-used
     *
     * @param parameters
     * @param createTableOrViewPreference
     * @throws AnalysisException
     */
    public void pipeline(@Nonnull Map<String, ?> parameters, @Nullable CreateTableOrViewPreference createTableOrViewPreference)
            throws AnalysisException {
        var tableOrViewName = getTableName(parameters, getViewName(parameters, this.getClass().getSimpleName()));

        assert StringUtils.isNotEmpty(tableOrViewName);

        var tableOrViewExists = tableOrViewExists(tableOrViewName);

        Dataset<Row> dataset = tableOrViewExists ? sparkSession.table(tableOrViewName) : buildDataset(parameters);

        if (Objects.nonNull(createTableOrViewPreference) && !tableOrViewExists) {
            createView(dataset, tableOrViewName, createTableOrViewPreference);
        }

        writeDataset(dataset, parameters);
    }

    /**
     * Write the aggregated {@link Dataset} to desired data source (say a RDBMS like Postgres). Need to be filled in by
     * the implementation.
     *
     * @param dataset
     * @param parameters
     */
    public abstract void writeDataset(@Nonnull Dataset<Row> dataset, @Nonnull Map<String, ?> parameters);

}
