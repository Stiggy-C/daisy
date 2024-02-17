package io.openenterprise.daisy.spark.sql;

import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;

import javax.annotation.Nonnull;
import java.util.Map;

@Getter
public enum Parameter implements io.openenterprise.Parameter {

    CSV_DELIMITER("daisy.spark.dataset.csv.delimiter", String.class),

    CSV_HEADER("daisy.spark.dataset.csv.header", Boolean.class),

    CSV_INFER_SCHEMA("daisy.spark.dataset.csv.infer-schema", Boolean.class),

    DATASET("daisy.spark.dataset", Dataset.class),

    DATASET_FORMAT("daisy.spark.dataset.format", String.class),

    DATASET_JOIN_COLUMNS("daisy.spark.dataset.join-columns", Pair.class),

    DATASET_JOIN_TYPE("daisy.spark.dataset.join-type", String.class),

    DATASET_PAIR("daisy.spark.dataset-pair", Pair.class),

    DATASET_PATH("daisy.spark.dataset.path", String.class),

    DATASET_SAVE_MODE("daisy.spark.dataset.save-mode", SaveMode.class),

    DATASET_SQL("daisy.spark.dataset.sql", String.class),

    DATASET_SQL_PARAMETERS("daisy.spark.dataset.sql.parameters", Map.class),

    DATASET_TABLE("daisy.spark.dataset.table", String.class),

    DATASET_VARIABLE("daisy.spark.dataset-variable", String.class),

    DATASET_VIEW("daisy.spark.dataset.view", String.class),

    DATASET_VIEW_GLOBAL("daisy.spark.dataset.view.global", Boolean.class),

    DATASET_VIEW_REPLACE("daisy.spark.dataset.view.replace", Boolean.class),

    JDBC_DB_TABLE("daisy.spark.dataset.jdbc.db-table", String.class),

    JDBC_DRIVER_CLASS("daisy.spark.dataset.jdbc.driver-class", String.class),

    JDBC_PASSWORD("diasy.spark.dataset.jdbc.password", String.class),

    JDBC_URL("daisy.spark.dataset.jdbc.url", String.class),

    JDBC_USER("daisy.spark.dataset.jdbc.user-name", String.class),

    JSON_MULTI_LINE("daisy.spark.dataset.json.multi-line", Boolean.class);

    private final String key;

    private final Class<?> valueType;

    Parameter(String key, Class<?> valueType) {
        this.key = key;
        this.valueType = valueType;
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public  <T> Class<T> getValueType() {
        return (Class<T>) valueType;
    }
}
