package io.openenterprise.daisy.spark.sql;

public enum CreateTableOrViewPreference {

    CREATE_GLOBAL_VIEW, CREATE_LOCAL_VIEW, CREATE_OR_REPLACE_GLOBAL_VIEW, CREATE_OR_REPLACE_LOCAL_VIEW,
    CREATE_TABLE_APPEND, CREATE_TABLE_ERROR_IF_EXISTS, CREATE_TABLE_IGNORE_IF_EXISTS, CREATE_TABLE_OVERWRITE;

    public boolean isGlobalView() {
        switch (this) {
            case CREATE_GLOBAL_VIEW:
            case CREATE_OR_REPLACE_GLOBAL_VIEW:
                return true;
            default:
                return false;
        }
    }
}
