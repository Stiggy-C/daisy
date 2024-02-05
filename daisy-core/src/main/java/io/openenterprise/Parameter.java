package io.openenterprise;

import javax.annotation.Nonnull;

public interface Parameter {

    String getKey();

    <T> Class<T> getValueType();

}

