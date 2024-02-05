package io.openenterprise.daisy.spark.ml;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.util.MLWritable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.inject.Inject;
import java.util.Map;

import static io.openenterprise.daisy.spark.ml.Parameter.ML_SAVE_MODEL;

public abstract class AbstractTrainOperationImpl<M extends Transformer & MLWritable>
        extends AbstractMLOperationImpl<M, M> implements TrainOperation<M> {

    @Inject
    protected ModelStorage modelStorage;

    @Nullable
    @Override
    public M invoke(@NotNull Map<String, Object> parameters) {
        return withInvocationContext(parameters, params -> {
            var dataset = getDataset(params);
            var model = train(dataset, parameters);

            if (params.containsKey(ML_SAVE_MODEL.getKey())) {
                var saveModel = BooleanUtils.toBoolean(MapUtils.getObject(parameters, ML_SAVE_MODEL.getKey(),
                        Boolean.FALSE).toString());

                if (saveModel) {
                    saveModel(model, params);
                }
            }

            return model;
        });
    }

    protected void saveModel(@NotNull M model, @NotNull Map<String, Object> parameters) {
        modelStorage.save(model);
    }
}
