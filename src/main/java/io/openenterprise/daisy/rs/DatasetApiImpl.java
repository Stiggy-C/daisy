package io.openenterprise.daisy.rs;

import io.openenterprise.daisy.rs.model.BuildDatasetResponse;
import io.openenterprise.daisy.rs.model.CreateTableOrViewPreference;
import io.openenterprise.daisy.spark.sql.AbstractBaseDatasetComponentImpl;
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
                                             @Nullable CreateTableOrViewPreference createTempViewPreference) {
        if (!applicationContext.containsBean(name)) {
            throw new NoSuchBeanDefinitionException(name);
        }

        var bean = applicationContext.getBean(name);

        assert bean instanceof AbstractBaseDatasetComponentImpl;

        var mappedCreateTempViewPreference= mapCreateTableOrViewPreference(createTempViewPreference);

        ((AbstractBaseDatasetComponentImpl) bean).buildDataset(parameters, mappedCreateTempViewPreference);

        boolean createdTable = Objects.nonNull(mappedCreateTempViewPreference) && mappedCreateTempViewPreference.isTable();
        boolean createdView = Objects.nonNull(mappedCreateTempViewPreference) && mappedCreateTempViewPreference.isView();
        BuildDatasetResponse.ViewTypeEnum viewType = createdView? mappedCreateTempViewPreference.isGlobalView()?
                BuildDatasetResponse.ViewTypeEnum.GLOBAL: BuildDatasetResponse.ViewTypeEnum.LOCAL : null;

        return new BuildDatasetResponse().createdTable(createdTable).createdView(createdView).viewType(viewType);
    }

    @Nullable
    private static io.openenterprise.daisy.spark.sql.CreateTableOrViewPreference mapCreateTableOrViewPreference(
            @Nullable CreateTableOrViewPreference createTableOrViewPreference) {
        return Objects.nonNull(createTableOrViewPreference) ?
                io.openenterprise.daisy.spark.sql.CreateTableOrViewPreference.valueOf(createTableOrViewPreference.toString()) :
                null;
    }
}
