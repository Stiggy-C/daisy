package io.openenterprise.daisy.spark.ml;

import io.openenterprise.daisy.Invocation;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.util.MLWritable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.Map;
import java.util.Objects;

public abstract class AbstractPredictOperationImpl<M extends Transformer & MLWritable>
        extends AbstractMLOperationImpl<M, Dataset<Row>> implements PredictOperation<M> {

    protected Class<M> modelClass;

    @Inject
    protected ModelStorage modelStorage;

    public AbstractPredictOperationImpl(Class<M> modelClass) {
        this.modelClass = modelClass;
    }

    @Nonnull
    @Override
    public Dataset<Row> predict(@Nonnull Dataset<Row> dataset, @Nonnull M model, @Nonnull Map<String, Object> parameters) {
        return model.transform(dataset);
    }

    @Nullable
    @Override
    public Dataset<Row> invoke(@Nonnull Map<String, Object> parameters) {
        return withInvocationContext(parameters, params -> {
            var dataset = getDataset(params);

            if (Objects.isNull(dataset)) {
                throw new IllegalStateException("dataset can not be null");
            }

            var model = loadModel(params);

            if (Objects.isNull(model)) {
                throw new IllegalStateException("model is null");
            }

            return predict(dataset, model, params);
        });
    }

    @Nullable
    protected abstract Dataset<Row> getDataset(@Nonnull Map<String, Object> parameters);

    @SuppressWarnings("unchecked")
    @Nullable
    protected M loadModel(@Nonnull Map<String, Object> parameters) {
        M model;

        if (parameters.containsKey(Parameter.ML_MODEL.getKey())) {
            model = (M) MapUtils.getObject(parameters, Parameter.ML_MODEL.getKey());
        } else if (parameters.containsKey(Parameter.ML_MODEL_ID.getKey())) {
            var modelId = MapUtils.getString(parameters, Parameter.ML_MODEL_ID.getKey());

            model = modelStorage.load(modelClass, modelId);
        } else {
            // Look for the model from previous invocations
            var sessionId = getSessionId(parameters);
            var invocationContext = getInvocationContext(sessionId);

            assert Objects.nonNull(invocationContext);

            return (M) invocationContext.getPastInvocations().stream()
                    .filter(invocation -> Objects.nonNull(invocation.getResult()))
                    .map(Invocation::getResult)
                    .filter(obj -> ClassUtils.isAssignable(obj.getClass(), modelClass))
                    .reduce((first, second) -> second)
                    .orElse(null);
        }

        return model;
    }

}
