package io.openenterprise.daisy.rs;

import io.openenterprise.daisy.rs.model.BuildDatasetResponse;
import io.openenterprise.daisy.rs.model.CreateTempViewPreference;
import io.openenterprise.daisy.spark.sql.AbstractSparkSqlService;
import io.openenterprise.daisy.spark.sql.CreateTableOrViewPreference;
import lombok.SneakyThrows;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.Map;
import java.util.Objects;

@Component
public class DatasetApiImpl implements DatasetApi {

    @Inject
    protected ApplicationContext applicationContext;

    @SneakyThrows
    @Override
    public BuildDatasetResponse buildDataset(@Nonnull Map<String, Object> parameters, @Nonnull String name,
                                             @Nonnull Boolean createTempView,
                                             @Nullable CreateTempViewPreference createTempViewPreference) {
        if (!applicationContext.containsBean(name)) {
            throw new NoSuchBeanDefinitionException(name);
        }

        var bean = applicationContext.getBean(name);

        assert bean instanceof AbstractSparkSqlService;

        var mappedCreateTempViewPreference
                = mapCreateTempViewPreference(createTempView, createTempViewPreference);

        ((AbstractSparkSqlService) bean).buildDataset(parameters, mappedCreateTempViewPreference);

        return new BuildDatasetResponse().createdView(createTempView).viewType(
                Objects.isNull(mappedCreateTempViewPreference)? null :
                        mappedCreateTempViewPreference.isGlobalView()? BuildDatasetResponse.ViewTypeEnum.GLOBAL :
                                BuildDatasetResponse.ViewTypeEnum.LOCAL);
    }

    @Nullable
    private static CreateTableOrViewPreference mapCreateTempViewPreference(
            @Nonnull Boolean createTempView, @Nullable CreateTempViewPreference createTempViewPreference) {
        return createTempView && Objects.nonNull(createTempViewPreference) ?
                CreateTableOrViewPreference.valueOf(createTempViewPreference.toString()) :
                null;
    }
}
