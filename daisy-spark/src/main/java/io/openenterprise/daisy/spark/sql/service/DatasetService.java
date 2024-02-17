package io.openenterprise.daisy.spark.sql.service;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Properties;

/**
 * Service which allows caller to interact with {@link Dataset} with ease. This is not serializable and should not be
 * used in any user defined function (UDF) of Apache Spark.
 */
public interface DatasetService extends BaseDatasetService<Void> {

    @Nonnull
    Dataset<Row> loadDataset(@Nonnull String format, @Nullable Map<String, String> options, @Nonnull String... paths);

    @Nonnull
    Dataset<Row> loadDataset(@Nonnull String jdbcUrl, @Nonnull String jdbcDbTable, @Nonnull Properties jdbcProperties,
                             @Nullable Map<String, String> options);

    @Nonnull
    Dataset<Row> loadDataset(@Nonnull String jdbcUrl, @Nonnull String jdbcDbTable, @Nonnull String jdbcUser,
                             @Nonnull String jdbcPassword, @Nullable String jdbcDriver,
                             @Nullable Map<String, String> options);

    void saveDataset(@Nonnull Dataset<Row> dataset, @Nonnull String table, @Nullable String format,
                     @Nullable Map<String, String> options, @Nullable SaveMode saveMode);

    void saveDataset(@Nonnull Dataset<Row> dataset, @Nonnull String view, boolean global, boolean replace)
            throws AnalysisException;

    void saveDatasetExternally(@Nonnull Dataset<Row> dataset, @Nonnull String path, @Nullable String format,
                               @Nullable Map<String, String> options, @Nonnull SaveMode saveMode);

    void saveDatasetExternally(@Nonnull Dataset<Row> dataset, @Nonnull String jdbcUrl, @Nonnull String jdbcDbTable,
                               @Nonnull Properties jdbcProperties, @Nullable Map<String, String> options,
                               @Nonnull SaveMode saveMode);

    void saveDatasetExternally(@Nonnull Dataset<Row> dataset, @Nonnull String jdbcUrl, @Nonnull String jdbcDbTable,
                               @Nonnull String jdbcUser, @Nonnull String jdbcPassword, @Nullable String jdbcDriver,
                               @Nullable Map<String, String> options, @Nonnull SaveMode saveMode);
}
