package io.openenterprise.daisy.spark;

import lombok.Getter;

public enum Constants {

    FORMAT_PARAMETER_NAME("daisy.spark.dataset.format"),

    PATH_PARAMETER_NAME("daisy.spark.dataset.path"),

    TABLE_NAME_PARAMETER_NAME("daisy.spark.dataset.table-name"),

    VIEW_NAME_PARAMETER_NAME("daisy.spark.dataset.view-name");

    @Getter
    private String value;

    Constants(String value) {
        this.value = value;
    }
}
