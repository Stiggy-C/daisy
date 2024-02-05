package io.openenterprise.daisy.spark.sql.execution.datasources.jdbc;

import lombok.Getter;

public enum JDBCOptions {

    PRIMARY_KEYS("primaryKeys"),

    UPSERT("upsert");

    @Getter
    private final String value;

    JDBCOptions(String value) {
        this.value = value;
    }
}
