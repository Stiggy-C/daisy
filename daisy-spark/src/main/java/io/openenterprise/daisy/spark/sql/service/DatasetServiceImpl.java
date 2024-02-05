package io.openenterprise.daisy.spark.sql.service;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.openenterprise.daisy.spark.sql.JdbcUtils;
import io.openenterprise.daisy.spark.sql.Parameter;
import io.openenterprise.daisy.spark.sql.execution.datasources.jdbc.MySqlJdbcRelationProvider;
import io.openenterprise.daisy.spark.sql.execution.datasources.jdbc.PostgreSqlJdbcRelationProvider;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.delta.sources.DeltaSourceUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.*;

import static io.openenterprise.daisy.spark.sql.Parameter.*;

@Service
public class DatasetServiceImpl implements DatasetService {

    protected static final String DEFAULT_FORMAT = DeltaSourceUtils.NAME();

    protected static final Set<String> JDBC_RELATED_FORMATS = Set.of("jdbc", MySqlJdbcRelationProvider.SHORT_NAME,
            PostgreSqlJdbcRelationProvider.SHORT_NAME);

    @Inject
    protected SparkSession sparkSession;

    @Nullable
    @SuppressWarnings("unchecked")
    @Override
    public Dataset<Row> loadDataset(@Nonnull Map<String, Object> parameters) {
        Dataset<Row> dataset = null;

        if (parameters.containsKey(DATASET.getKey())) {
            dataset = (Dataset<Row>) MapUtils.getObject(parameters, DATASET.getKey());
        } else if (parameters.containsKey(DATASET_FORMAT.getKey())) {
            var format = getFormat(parameters);

            if (JDBC_RELATED_FORMATS.contains("format")) {
                if (parameters.containsKey(JDBC_URL.getKey()) &&
                        parameters.containsKey(JDBC_DB_TABLE.getKey())) {
                    var jdbcDbTable = ObjectUtils.requireNonEmpty(MapUtils.getString(parameters,
                            JDBC_DB_TABLE.getKey()));
                    var jdbcUrl = ObjectUtils.requireNonEmpty(MapUtils.getString(parameters,
                            JDBC_URL.getKey()));

                    dataset = loadDataset(jdbcUrl, jdbcDbTable, JdbcUtils.createConnectionProperties(parameters));
                } else {
                    throw new IllegalArgumentException("Missing jdbc related param's");
                }
            } else {
                var options = Maps.<String, String>newHashMap();
                var path = getPath(parameters);

                switch (format) {
                    case "csv":
                        if (parameters.containsKey(CSV_DELIMITER.getKey())) {
                            var delimiter = MapUtils.getString(parameters, CSV_DELIMITER.getKey(), ",");

                            options.put("delimiter", delimiter);
                        }

                        if (parameters.containsKey(CSV_HEADER.getKey())) {
                            var header = BooleanUtils.toBoolean(
                                    MapUtils.getString(parameters, CSV_HEADER.getKey(), "false"));

                            options.put("header", BooleanUtils.toStringTrueFalse(header));
                        }

                        if (parameters.containsKey(CSV_INFER_SCHEMA.getKey())) {
                            var inferSchema = BooleanUtils.toBoolean(
                                    MapUtils.getString(parameters, CSV_HEADER.getKey(), "false"));

                            options.put("inferSchema", BooleanUtils.toStringTrueFalse(inferSchema));
                        }
                        break;
                    case "json":
                        if (parameters.containsKey(JSON_MULTI_LINE.getKey())) {
                            var multiLine = BooleanUtils.toBoolean(
                                    MapUtils.getString(parameters, JSON_MULTI_LINE.getKey(), "false"));

                            options.put("multiLine", BooleanUtils.toStringTrueFalse(multiLine));
                        }

                        break;
                    default:
                }

                dataset = loadDataset(format, options, path);
            }
        } else if (parameters.containsKey(DATASET_TABLE.getKey())) {
            dataset = loadDataset(MapUtils.getString(parameters, DATASET_TABLE.getKey()));
        } else if (parameters.containsKey(DATASET_VIEW.getKey())) {
            dataset = loadDataset(MapUtils.getString(parameters, DATASET_VIEW.getKey()));
        }

        return dataset;
    }


    @Nonnull
    @Override
    public Dataset<Row> loadDataset(@Nonnull String format, @Nullable Map<String, String> options, @Nonnull String path) {
        return this.loadDataset(format, options, new String[] {path});
    }

    @Nonnull
    @Override
    public Dataset<Row> loadDataset(@Nonnull String format, @Nullable Map<String, String> options, @Nonnull String... paths) {
        var dataFrameReader = sparkSession.read().format(format);

        if (Objects.nonNull(options)) {
            dataFrameReader = dataFrameReader.options(options);
        }

        return dataFrameReader.load(paths);
    }

    @Nonnull
    @Override
    public Dataset<Row> loadDataset(@Nonnull String jdbcUrl, @Nonnull String jdbcDbTable,
                                    @Nonnull Properties connectionProperties) {
        return sparkSession.read().jdbc(jdbcUrl, jdbcDbTable, connectionProperties);
    }

    @Nonnull
    @Override
    public Dataset<Row> loadDataset(@Nonnull String jdbcUrl, @Nonnull String jdbcDbTable, @Nonnull String jdbcUser,
                                    @Nonnull String jdbcPassword, @Nullable String jdbcDriver) {
        return loadDataset(jdbcUrl, jdbcDbTable, JdbcUtils.createConnectionProperties(jdbcUser, jdbcPassword,
                jdbcDriver));
    }

    @Nonnull
    @Override
    public Dataset<Row> loadDataset(@Nonnull String tableOrView) {
        return sparkSession.table(tableOrView);
    }

    @Override
    public void saveDataset(@Nonnull Dataset<Row> dataset, @Nonnull Map<String, Object> parameters) throws AnalysisException {
        SaveMode saveMode = parameters.containsKey(DATASET_SAVE_MODE.getKey()) ?
                determineSaveMode(MapUtils.getObject(parameters, DATASET_SAVE_MODE.getKey())) : null;

        if (parameters.containsKey(DATASET_FORMAT.getKey())) {
            var format = getFormat(parameters);

            switch (format) {
                case "jdbc":
                case MySqlJdbcRelationProvider.SHORT_NAME:
                case PostgreSqlJdbcRelationProvider.SHORT_NAME:
                    var jdbcDbTable = ObjectUtils.requireNonEmpty(MapUtils.getString(parameters,
                            JDBC_DB_TABLE.getKey()));
                    var jdbcUrl = ObjectUtils.requireNonEmpty(MapUtils.getString(parameters,
                            JDBC_URL.getKey()));

                    saveDatasetExternally(dataset, jdbcUrl, jdbcDbTable, JdbcUtils.createConnectionProperties(parameters),
                            saveMode);

                    break;
                default:
                    if (parameters.containsKey(DATASET_PATH.getKey())) {
                        var path = MapUtils.getString(parameters, DATASET_PATH.getKey());

                        saveDatasetExternally(dataset, format, path, saveMode);
                    } else if (parameters.containsKey(DATASET_TABLE.getKey())) {
                        var table = MapUtils.getString(parameters, DATASET_TABLE.getKey());

                        saveDataset(dataset, table, format, saveMode);
                    } else {
                        throw new UnsupportedOperationException();
                    }
            }
        } else if (parameters.containsKey(DATASET_VIEW.getKey())) {
            var view = MapUtils.getString(parameters, DATASET_VIEW.getKey());
            var global = MapUtils.getBoolean(parameters, DATASET_VIEW_GLOBAL.getKey(),
                    false);
            var replace = MapUtils.getBoolean(parameters, DATASET_VIEW_REPLACE.getKey(),
                    false);

            saveDataset(dataset, view, global, replace);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void saveDataset(@Nonnull Dataset<Row> dataset, @Nonnull String table, @Nullable String format,
                            @Nullable SaveMode saveMode) {
        var dataFrameWriter = dataset.write().format(StringUtils.isEmpty(format)? DEFAULT_FORMAT : format);

        if (Objects.nonNull(saveMode)) {
            dataFrameWriter = dataFrameWriter.mode(saveMode);
        }

        dataFrameWriter.saveAsTable(table);
    }

    @Override
    public void saveDataset(@Nonnull Dataset<Row> dataset, @Nonnull String view, boolean global, boolean replace)
            throws AnalysisException {
        if (global && replace) {
            dataset.createOrReplaceGlobalTempView(view);
        } else if (global) {
            dataset.createGlobalTempView(view);
        } else if (replace) {
            dataset.createOrReplaceTempView(view);
        } else {
            dataset.createTempView(view);
        }
    }

    @Override
    public void saveDatasetExternally(@Nonnull Dataset<Row> dataset, @Nullable String format, @Nonnull String path,
                                      @Nullable SaveMode saveMode) {
        var dataFrameWriter = dataset.write().format(StringUtils.isEmpty(format)? DEFAULT_FORMAT : format);

        if (Objects.nonNull(saveMode)) {
            dataFrameWriter = dataFrameWriter.mode(saveMode);
        }

        dataFrameWriter.save(path);
    }

    @Override
    public void saveDatasetExternally(@Nonnull Dataset<Row> dataset, @Nonnull String jdbcUrl, @Nonnull String jdbcDbTable,
                                      @Nonnull Properties connectionProperties, @Nullable SaveMode saveMode) {
        var dataFrameWriter = dataset.write();

        if (Objects.nonNull(saveMode)) {
            dataFrameWriter = dataFrameWriter.mode(saveMode);
        }

        dataFrameWriter.jdbc(jdbcUrl, jdbcDbTable, connectionProperties);
    }

    @Override
    public void saveDatasetExternally(@Nonnull Dataset<Row> dataset, @Nonnull String jdbcUrl, @Nonnull String jdbcDbTable,
                                      @Nonnull String jdbcUser, @Nonnull String jdbcPassword, @Nullable String jdbcDriver,
                                      @Nullable SaveMode saveMode) {
        saveDatasetExternally(dataset, jdbcUrl, jdbcDbTable, JdbcUtils.createConnectionProperties(
                jdbcUser, jdbcPassword, jdbcDriver), saveMode);
    }

    @Nullable
    private SaveMode determineSaveMode(@Nullable Object object) {
        if (Objects.isNull(object)) {
            return null;
        }

        if (ClassUtils.isAssignable(object.getClass(), SaveMode.class)) {
            return (SaveMode) object;
        } else if (ClassUtils.isAssignable(object.getClass(), String.class)) {
            var string = object.toString();

            return StringUtils.isEmpty(string) ? SaveMode.ErrorIfExists :
                    SaveMode.valueOf(StringUtils.capitalize(StringUtils.lowerCase(string)));
        } else {
            throw new UnsupportedOperationException("Unable to determine SaveMode");
        }
    }

    @Nonnull
    private String getFormat(@Nonnull Map<String, Object> parameters) {
        if (parameters.containsKey(DATASET_FORMAT.getKey())) {
            return MapUtils.getString(parameters, DATASET_FORMAT.getKey());
        } else {
            throw new IllegalArgumentException(DATASET_FORMAT.getKey() + " is missing");
        }
    }

    @Nonnull
    private String getPath(@Nonnull Map<String, Object> parameters) {
        if (parameters.containsKey(DATASET_PATH.getKey())) {
            return MapUtils.getString(parameters, DATASET_PATH.getKey());
        } else {
            throw new IllegalArgumentException(DATASET_PATH.getKey() + " is missing");
        }
    }
}
