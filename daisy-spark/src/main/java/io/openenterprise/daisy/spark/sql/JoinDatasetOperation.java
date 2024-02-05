package io.openenterprise.daisy.spark.sql;

import io.openenterprise.daisy.Operation;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

@SuppressWarnings("rawtypes")
public interface JoinDatasetOperation extends Operation<Dataset> {

    @Nullable
    Dataset<?> join(@Nonnull Map<String, Object> parameters);
}
