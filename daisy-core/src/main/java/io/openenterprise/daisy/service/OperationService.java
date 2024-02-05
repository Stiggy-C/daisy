package io.openenterprise.daisy.service;

import io.openenterprise.daisy.Operation;

import javax.annotation.Nonnull;
import java.util.Map;

public interface OperationService {

    <T> T invoke (@Nonnull String beanNameOrClassName, @Nonnull Map<String, Object> parameters);

    <T> T invoke (@Nonnull Operation<T> operation, @Nonnull Map<String, Object> parameters);

}
