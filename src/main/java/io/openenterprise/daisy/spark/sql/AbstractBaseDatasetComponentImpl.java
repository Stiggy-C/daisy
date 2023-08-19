package io.openenterprise.daisy.spark.sql;

import io.openenterprise.daisy.Parameters;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.Map;
import java.util.Objects;

public abstract class AbstractBaseDatasetComponentImpl implements BaseDatasetComponent {

    @Inject
    protected SparkSession sparkSession;

    /**
     * Built the aggregated {@link Dataset} from different data sources (say a RDBMS like Postgres) and create a temp
     * view of the {@link Dataset} to be re-used if necessary.
     *
     * @param parameters
     * @return
     */
    @Nonnull
    public Dataset<Row> buildDataset(
            @Nonnull Map<String, ?> parameters, @Nullable CreateTableOrViewPreference createTableOrViewPreference)
            throws AnalysisException {
        var tableOrViewName = getTableName(parameters, getViewName(parameters, this.getClass().getSimpleName()));

        assert StringUtils.isNotEmpty(tableOrViewName);

        var tableOrViewExists = tableOrViewExists(tableOrViewName);

        Dataset<Row> dataset = tableOrViewExists ? sparkSession.table(tableOrViewName) : buildDataset(parameters);

        if (Objects.nonNull(createTableOrViewPreference) && !tableOrViewExists) {
            switch (createTableOrViewPreference) {
                case CREATE_TABLE_APPEND:
                case CREATE_TABLE_ERROR_IF_EXISTS:
                case CREATE_TABLE_IGNORE_IF_EXISTS:
                case CREATE_TABLE_OVERWRITE:
                    var format = getFormat(parameters, null);
                    var path = getPath(parameters, null);

                    createTable(dataset, tableOrViewName, format, path, createTableOrViewPreference);
                    break;
                default:
                    createView(dataset, tableOrViewName, createTableOrViewPreference);
            }

        }

        return dataset;
    }

    /**
     * Built the aggregated {@link Dataset} from different data sources (say a RDBMS like Postgres).
     *
     * @param parameters
     * @return
     */
    @Nonnull
    public abstract Dataset<Row> buildDataset(@Nonnull Map<String, ?> parameters);

    protected void createTable(@Nonnull Dataset<Row> dataset, @Nonnull String tableOrViewName, @Nullable String format,
                               @Nullable String path, @Nonnull CreateTableOrViewPreference createTableOrViewPreference) {
        var dataFrameWriter = dataset.write();

        if (StringUtils.isNotEmpty(format)) {
            dataFrameWriter = dataFrameWriter.format(format);
        }

        if (StringUtils.isNotEmpty(path)) {
            dataFrameWriter.option("path", path);
        }

        switch (createTableOrViewPreference) {
            case CREATE_TABLE_APPEND:
                dataFrameWriter.mode(SaveMode.Append);
                break;
            case CREATE_TABLE_ERROR_IF_EXISTS:
                dataFrameWriter.mode(SaveMode.ErrorIfExists);
                break;
            case CREATE_TABLE_IGNORE_IF_EXISTS:
                dataFrameWriter.mode(SaveMode.Ignore);
                break;
            case CREATE_TABLE_OVERWRITE:
                dataFrameWriter.mode(SaveMode.Overwrite);
                break;
            default:
                throw new UnsupportedOperationException();
        }

        if (StringUtils.isNotEmpty(format) && isExternalDeltaTable(format, path)) {
            dataFrameWriter.save();
        } else {
            dataFrameWriter.saveAsTable(tableOrViewName);
        }
    }

    protected void createView(
            @Nonnull Dataset<Row> dataset, @Nonnull String viewName,
            @Nonnull CreateTableOrViewPreference createTableOrViewPreference) throws AnalysisException {
        switch (createTableOrViewPreference) {
            case CREATE_GLOBAL_VIEW:
                dataset.createGlobalTempView(viewName);
                break;
            case CREATE_LOCAL_VIEW:
                dataset.createTempView(viewName);
                break;
            case CREATE_OR_REPLACE_GLOBAL_VIEW:
                dataset.createOrReplaceGlobalTempView(viewName);
                break;
            case CREATE_OR_REPLACE_LOCAL_VIEW:
                dataset.createOrReplaceTempView(viewName);
                break;
        }
    }

    @Nonnull
    protected String getBeanName() {
        var componentAnnotation = getClass().getAnnotation(Component.class);
        var serviceAnnotation = getClass().getAnnotation(Service.class);

        return Objects.nonNull(componentAnnotation)? componentAnnotation.value() :
                Objects.nonNull(serviceAnnotation) ? serviceAnnotation.value() :
                        StringUtils.uncapitalize(getClass().getSimpleName());
    }

    @Nonnull
    protected String getFormat(@Nonnull Map<String, ?> parameters) {
        var format = getFormat(parameters, "parquet");

        assert StringUtils.isNotEmpty(format);

        return format;
    }

    @Nullable
    protected String getFormat(@Nonnull Map<String, ?> parameters, @Nullable String defaultValue) {
        return MapUtils.getString(parameters, Parameters.DATASET_FORMAT.getName(), defaultValue);
    }

    @Nonnull
    protected String getPath(@Nonnull Map<String, ?> parameters) {
        var path = getPath(parameters, null);

        if (StringUtils.isEmpty(path)) {
            throw new IllegalArgumentException(Parameters.DATASET_PATH.getName() + " is missing");
        }

        return path;
    }

    @Nullable
    protected String getPath(@Nonnull Map<String, ?> parameters, @Nullable String defaultValue) {
        return MapUtils.getString(parameters, Parameters.DATASET_PATH.getName(), defaultValue);
    }

    @Nonnull
    protected String getTableName(@Nonnull Map<String, ?> parameters) {
        var tableName = getTableName(parameters, "default." + StringUtils.uncapitalize(
                this.getClass().getSimpleName()));

        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException(Parameters.DATASET_TABLE.getName() + " is missing");
        }

        return tableName;
    }

    @Nullable
    protected String getTableName(@Nonnull Map<String, ?> parameters, @Nullable String defaultValue) {
        return MapUtils.getString(parameters, Parameters.DATASET_TABLE.getName(), defaultValue);
    }

    @Nonnull
    protected String getViewName(@Nonnull Map<String, ?> parameters) {
        var viewName = getViewName(parameters, "default." + StringUtils.uncapitalize(
                this.getClass().getSimpleName()));

        if (StringUtils.isEmpty(viewName)) {
            throw new IllegalArgumentException(Parameters.DATASET_VIEW.getName() + " is missing");
        }

        return viewName;
    }

    @Nullable
    protected String getViewName(@Nonnull Map<String, ?> parameters, @Nullable String defaultValue) {
        return MapUtils.getString(parameters, Parameters.DATASET_VIEW.getName(), defaultValue);
    }

    protected boolean isExternalDeltaTable(@Nonnull Map<String, ?> parameters) {
        return StringUtils.equals("delta", getFormat(parameters, null)) &&
                StringUtils.isNotEmpty(getPath(parameters, null));
    }

    protected boolean isExternalDeltaTable(@Nonnull String format, @Nullable String path) {
        return StringUtils.equals("delta", format) && StringUtils.isNotEmpty(path);
    }

    @Nonnull
    protected Dataset<Row> loadTableOrView(@Nonnull Map<String, ?> parameters) {
        return isExternalDeltaTable(parameters) ?
                sparkSession.read().format("delta").option("path", getPath(parameters)).load() :
                sparkSession.table(getTableName(parameters));
    }

    protected boolean tableOrViewExists(@Nonnull String tableOrViewName) {
        return sparkSession.catalog().tableExists(tableOrViewName);
    }
}