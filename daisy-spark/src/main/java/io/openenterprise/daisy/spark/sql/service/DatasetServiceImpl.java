package io.openenterprise.daisy.spark.sql.service;

import com.google.common.collect.Maps;
import io.openenterprise.daisy.spark.sql.JdbcUtils;
import io.openenterprise.daisy.spark.sql.execution.datasources.jdbc.MySqlJdbcRelationProvider;
import io.openenterprise.daisy.spark.sql.execution.datasources.jdbc.PostgreSqlJdbcRelationProvider;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import static io.openenterprise.daisy.spark.sql.Parameter.*;


@Service("datasetService")
public class DatasetServiceImpl extends AbstractBaseDatasetServiceImpl<Void> implements DatasetService {

    @Inject
    protected ApplicationContext applicationContext;

    @Nullable
    @SuppressWarnings("unchecked")
    @Override
    public Dataset<Row> loadDataset(@Nonnull Map<String, Object> parameters) {
        Dataset<Row> dataset = null;

        verifyParametersForLoadingDataset(parameters);

        if (parameters.containsKey(DATASET.getKey())) {
            dataset = (Dataset<Row>) MapUtils.getObject(parameters, DATASET.getKey());
        } else if (parameters.containsKey(DATASET_FORMAT.getKey())) {
            var format = getFormat(parameters);

            if (getSupportedJdbcBasedFormats().contains("format")) {
                if (parameters.containsKey(JDBC_URL.getKey()) &&
                        parameters.containsKey(JDBC_DB_TABLE.getKey())) {
                    var jdbcDbTable = ObjectUtils.requireNonEmpty(MapUtils.getString(parameters,
                            JDBC_DB_TABLE.getKey()));
                    var jdbcUrl = ObjectUtils.requireNonEmpty(MapUtils.getString(parameters,
                            JDBC_URL.getKey()));

                    dataset = loadDataset(jdbcUrl, jdbcDbTable, JdbcUtils.createConnectionProperties(parameters),
                            Maps.newHashMap());
                } else {
                    throw new IllegalArgumentException("Missing jdbc related param's");
                }
            } else {
                var options = getOptions(parameters);
                var path = getPath(parameters);

                dataset = loadDataset(format, options, path);
            }
        } else if (parameters.containsKey(DATASET_TABLE.getKey())) {
            dataset = loadDataset(MapUtils.getString(parameters, DATASET_TABLE.getKey()));
        } else if (parameters.containsKey(DATASET_VIEW.getKey())) {
            dataset = loadDataset(MapUtils.getString(parameters, DATASET_VIEW.getKey()));
        } else {
            throw new IllegalStateException();
        }

        return dataset;
    }

    @Nonnull
    @Override
    public Dataset<Row> loadDataset(@Nonnull String format, @Nullable Map<String, String> options, @Nonnull String path) {
        return this.loadDataset(format, options, new String[]{path});
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
                                    @Nonnull Properties jdbcProperties, @Nullable Map<String, String> options) {
        return sparkSession.read().options(Objects.isNull(options) ? Maps.newHashMap() : options)
                .jdbc(jdbcUrl, jdbcDbTable, jdbcProperties);
    }

    @Nonnull
    @Override
    public Dataset<Row> loadDataset(@Nonnull String jdbcUrl, @Nonnull String jdbcDbTable, @Nonnull String jdbcUser,
                                    @Nonnull String jdbcPassword, @Nullable String jdbcDriver, @Nullable Map<String, String> options) {
        return loadDataset(jdbcUrl, jdbcDbTable, JdbcUtils.createConnectionProperties(jdbcUser, jdbcPassword, jdbcDriver),
                options);
    }

    @Nonnull
    @Override
    public Dataset<Row> loadDataset(@Nonnull String tableOrView) {
        return sparkSession.table(tableOrView);
    }

    @Override
    public Void saveDataset(@Nonnull Dataset<Row> dataset, @Nonnull Map<String, Object> parameters) throws AnalysisException {
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
                            Maps.newHashMap(), saveMode);

                    break;
                default:
                    if (parameters.containsKey(DATASET_PATH.getKey())) {
                        var path = MapUtils.getString(parameters, DATASET_PATH.getKey());

                        saveDatasetExternally(dataset, path, format, Maps.newHashMap(), saveMode);
                    } else if (parameters.containsKey(DATASET_TABLE.getKey())) {
                        var table = MapUtils.getString(parameters, DATASET_TABLE.getKey());

                        saveDataset(dataset, table, format, Maps.newHashMap(), saveMode);
                    } else {
                        throw new UnsupportedOperationException();
                    }
            }
        } else if (parameters.containsKey(DATASET_TABLE.getKey())) {
            var table = MapUtils.getString(parameters, DATASET_TABLE.getKey());

            saveDataset(dataset, table, null, Maps.newHashMap(), saveMode);
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

        return null;
    }

    @Override
    public void saveDataset(@Nonnull Dataset<Row> dataset, @Nonnull String table, @Nullable String format,
                            @Nullable Map<String, String> options, @Nullable SaveMode saveMode) {
        var dataFrameWriter = getDataFrameWriter(dataset, format, options, saveMode);

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
    public void saveDatasetExternally(@Nonnull Dataset<Row> dataset, @Nonnull String path, @Nullable String format,
                                      @Nullable Map<String, String> options, @Nullable SaveMode saveMode) {
        getDataFrameWriter(dataset, format, options, saveMode).save(path);
    }

    @Override
    public void saveDatasetExternally(@Nonnull Dataset<Row> dataset, @Nonnull String jdbcUrl, @Nonnull String jdbcDbTable,
                                      @Nonnull Properties jdbcProperties, @Nullable Map<String, String> options,
                                      @Nullable SaveMode saveMode) {
        var dataFrameWriter = dataset.write();

        if (Objects.nonNull(saveMode)) {
            dataFrameWriter = dataFrameWriter.mode(saveMode);
        }

        dataFrameWriter.jdbc(jdbcUrl, jdbcDbTable, jdbcProperties);
    }

    @Override
    public void saveDatasetExternally(@Nonnull Dataset<Row> dataset, @Nonnull String jdbcUrl, @Nonnull String jdbcDbTable,
                                      @Nonnull String jdbcUser, @Nonnull String jdbcPassword, @Nullable String jdbcDriver,
                                      @Nullable Map<String, String> options, @Nullable SaveMode saveMode) {
        saveDatasetExternally(dataset, jdbcUrl, jdbcDbTable, JdbcUtils.createConnectionProperties(jdbcUser, jdbcPassword,
                jdbcDriver), options, saveMode);
    }

    @Nullable
    @SuppressWarnings("unchecked")
    protected Set<String> getSupportedJdbcBasedFormats() {
        return applicationContext.getBean("supportedJdbcBasedFormats", Set.class);
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


    private DataFrameWriter<Row> getDataFrameWriter(@NotNull Dataset<Row> dataset, @Nullable String format,
                                                    @Nullable Map<String, String> options, @Nullable SaveMode saveMode) {
        var dataFrameWriter = dataset.write();

        if (StringUtils.isNotEmpty(format)) {
            dataFrameWriter = dataFrameWriter.format(format);
        }

        if (Objects.nonNull(saveMode)) {
            dataFrameWriter = dataFrameWriter.mode(saveMode);
        }

        dataFrameWriter = dataFrameWriter.options(Objects.isNull(options) ? Maps.newHashMap() : options);

        return dataFrameWriter;
    }
}
