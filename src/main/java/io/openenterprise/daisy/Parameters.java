package io.openenterprise.daisy;

import lombok.Getter;

public enum Parameters {

    DATASET_FORMAT("daisy.spark.dataset.format", String.class),

    DATASET_PATH("daisy.spark.dataset.path", String.class),

    DATASET_SQL_STATEMENT("daisy.spark.dataset.sql", String.class),

    DATASET_TABLE("daisy.spark.dataset.table-name", String.class),

    DATASET_VIEW("daisy.spark.dataset.view-name", String.class),

    MVEL_CLASS_IMPORTS("daisy.mvel.class-imports", String[].class),

    MVEL_PACKAGE_IMPORTS("daisy.mvel.package-imports", String[].class),

    PLOT_PATH("daisy.plot.path", String.class),

    PLOT_TITLE("daisy.plot.title", String.class),

    SESSION_ID("daisy.session.id", String.class);

    @Getter
    private final String name;

    @Getter
    private final Class<?> valueType;

    Parameters(String name, Class<?> valueType) {
        this.name = name;
        this.valueType = valueType;
    }
}
