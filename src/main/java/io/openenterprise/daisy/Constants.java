package io.openenterprise.daisy;

import lombok.Getter;

public enum Constants {

    FORMAT_PARAMETER_NAME("daisy.spark.dataset.format"),

    PATH_PARAMETER_NAME("daisy.spark.dataset.path"),

    PLOT_PATH_PARAMETER_NAME("daisy.plot.path"),

    PLOT_TITLE_PARAMETER_NAME("daisy.plot.title"),

    SESSION_ID_PARAMETER_NAME("daisy.session.id"),

    SQL_STATEMENT_PARAMETER_NAME("daisy.spark.dataset.sql"),

    TABLE_NAME_PARAMETER_NAME("daisy.spark.dataset.table-name"),

    VIEW_NAME_PARAMETER_NAME("daisy.spark.dataset.view-name");



    @Getter
    private String value;

    Constants(String value) {
        this.value = value;
    }
}
