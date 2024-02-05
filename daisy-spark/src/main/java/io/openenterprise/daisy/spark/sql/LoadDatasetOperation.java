package io.openenterprise.daisy.spark.sql;

import io.openenterprise.daisy.Operation;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

@SuppressWarnings("rawtypes")
public interface LoadDatasetOperation extends Operation<Dataset> {

    @Nullable
    Dataset<?> load(@Nonnull Map<String, Object> parameters);
}
