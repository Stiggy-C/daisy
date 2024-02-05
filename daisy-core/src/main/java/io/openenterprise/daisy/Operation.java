package io.openenterprise.daisy;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

public interface Operation<T> {

    @Nullable
    T invoke(@Nonnull Map<String, Object> parameters);
}
