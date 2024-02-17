package io.openenterprise.daisy.spark.sql.service;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.Map;

import static io.openenterprise.daisy.spark.sql.Parameter.*;
import static io.openenterprise.daisy.spark.sql.Parameter.DATASET_TABLE;

public abstract class AbstractBaseDatasetServiceImpl<T> implements BaseDatasetService<T> {

    @Inject
    protected SparkSession sparkSession;

    @Nonnull
    protected String getFormat(@Nonnull Map<String, Object> parameters) {
        if (parameters.containsKey(DATASET_FORMAT.getKey())) {
            return MapUtils.getString(parameters, DATASET_FORMAT.getKey());
        } else {
            throw new IllegalArgumentException(DATASET_FORMAT.getKey() + " is missing");
        }
    }

    @Nonnull
    protected Map<String, String> getCsvOptions(@NotNull Map<String, Object> parameters) {
        var options = Maps.<String, String>newHashMap();

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

        return options;
    }

    @Nonnull
    protected Map<String, String> getJsonOptions(@NotNull Map<String, Object> parameters) {
        var options = Maps.<String, String>newHashMap();

        if (parameters.containsKey(JSON_MULTI_LINE.getKey())) {
            var multiLine = BooleanUtils.toBoolean(
                    MapUtils.getString(parameters, JSON_MULTI_LINE.getKey(), "false"));

            options.put("multiLine", BooleanUtils.toStringTrueFalse(multiLine));
        }

        return options;
    }

    @Nonnull
    protected Map<String, String> getOptions(@NotNull Map<String, Object> parameters) {
        var format = getFormat(parameters);

        Map<String, String> options;

        switch (format) {
            case "csv":
                options = getCsvOptions(parameters);
                break;
            case "json":
                options = getJsonOptions(parameters);
                break;
            default:
                options = Maps.newHashMap();
        }

        return options;
    }

    @Nonnull
    protected String getPath(@Nonnull Map<String, Object> parameters) {
        if (parameters.containsKey(DATASET_PATH.getKey())) {
            return MapUtils.getString(parameters, DATASET_PATH.getKey());
        } else {
            throw new IllegalArgumentException(DATASET_PATH.getKey() + " is missing");
        }
    }

    protected void verifyParametersForLoadingDataset(@Nonnull Map<String, Object> parameters) {
        var datasetRelatedParameterKeys = Sets.newHashSet(DATASET.getKey(), DATASET_FORMAT.getKey(),
                DATASET_TABLE.getKey(), DATASET_VIEW.getKey());
        var countOfDatasetRelatedParameterKeys = parameters.keySet().stream()
                .filter(datasetRelatedParameterKeys::contains).count();

        if (parameters.containsKey(DATASET_FORMAT.getKey()) && parameters.containsKey(DATASET_TABLE.getKey())) {
            countOfDatasetRelatedParameterKeys --;
        }

        if (countOfDatasetRelatedParameterKeys > 1) {
            throw new IllegalArgumentException("Too many dataset related parameter");
        }
    }

}
