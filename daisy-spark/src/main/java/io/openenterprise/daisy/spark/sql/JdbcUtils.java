package io.openenterprise.daisy.spark.sql;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Properties;

public final class JdbcUtils {

    private JdbcUtils() {}

    @Nonnull
    public static Properties createConnectionProperties(@Nonnull Map<String, Object> parameters) {
        var jdbcDriver = MapUtils.getString(parameters, io.openenterprise.daisy.spark.sql.Parameter.JDBC_DRIVER_CLASS.getKey());
        var jdbcPassword = ObjectUtils.requireNonEmpty(MapUtils.getString(parameters,
                io.openenterprise.daisy.spark.sql.Parameter.JDBC_PASSWORD.getKey()));
        var jdbcUser = ObjectUtils.requireNonEmpty(MapUtils.getString(parameters,
                Parameter.JDBC_USER.getKey()));

        return createConnectionProperties(jdbcUser, jdbcPassword, jdbcDriver);
    }

    @Nonnull
    public static Properties createConnectionProperties(@Nonnull String jdbcUser, @Nonnull String jdbcPassword,
                                                        @Nullable String jdbcDriver) {

        var connectionProperties = new Properties();

        if (StringUtils.isNotEmpty(jdbcDriver)) {
            connectionProperties.setProperty("driver", jdbcDriver);
        }

        connectionProperties.setProperty("password", jdbcPassword);
        connectionProperties.setProperty("user", jdbcUser);

        return connectionProperties;
    }
}
