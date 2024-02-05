package io.openenterprise.daisy.spark.sql.service;


import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Properties;

public interface DatasetService {

    @Nullable
    Dataset<Row> loadDataset(@Nonnull Map<String, Object> parameters);

    @Nonnull
    Dataset<Row> loadDataset(@Nonnull String format, @Nullable Map<String, String> options, @Nonnull String path);

    @Nonnull
    Dataset<Row> loadDataset(@Nonnull String format, @Nullable Map<String, String> options, @Nonnull String... paths);

    @Nonnull
    Dataset<Row> loadDataset(@Nonnull String jdbcUrl, @Nonnull String jdbcDbTable, @Nonnull Properties connectionProperties);

    @Nonnull
    Dataset<Row> loadDataset(@Nonnull String jdbcUrl, @Nonnull String jdbcDbTable, @Nonnull String jdbcUser,
                             @Nonnull String jdbcPassword, @Nullable String jdbcDriver);

    @Nonnull
    Dataset<Row> loadDataset(@Nonnull String tableOrView);

    void saveDataset(@Nonnull Dataset<Row> dataset, @Nonnull Map<String, Object> parameters) throws AnalysisException;

    void saveDataset(@Nonnull Dataset<Row> dataset, @Nonnull String table, @Nullable String format,
                     @Nullable SaveMode saveMode);

    void saveDataset(@Nonnull Dataset<Row> dataset, @Nonnull String view, boolean global, boolean replace) throws AnalysisException;

    void saveDatasetExternally(@Nonnull Dataset<Row> dataset, @Nullable String format, @Nonnull String path,
                               @Nonnull SaveMode saveMode);

    void saveDatasetExternally(@Nonnull Dataset<Row> dataset,  @Nonnull String jdbcUrl, @Nonnull String jdbcDbTable,
                               @Nonnull Properties connectionProperties, @Nonnull SaveMode saveMode);

    void saveDatasetExternally(@Nonnull Dataset<Row> dataset, @Nonnull String jdbcUrl, @Nonnull String jdbcDbTable,
                               @Nonnull String jdbcUser, @Nonnull String jdbcPassword, @Nullable String jdbcDriver,
                               @Nonnull SaveMode saveMode);
}
