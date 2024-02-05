package io.openenterprise.daisy;

import javax.annotation.Nonnull;
import java.util.Map;

public interface MvelDrivenOperation<T> extends Operation<T> {

    T eval(@Nonnull Map<String, Object> parameters);

}
