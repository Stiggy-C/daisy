package io.openenterprise.daisy.rs;

import io.openenterprise.daisy.rs.model.TrainingResponse;
import io.openenterprise.daisy.spark.ml.AbstractMachineLearning;
import io.openenterprise.daisy.spark.ml.ModelStorage;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class MlApiImpl implements MlApi {

    @Inject
    protected ApplicationContext applicationContext;

    @Inject
    protected ModelStorage modelStorage;

    @SuppressWarnings("unchecked")
    @Override
    public String getPrediction(@Nonnull String jsonString, @Nonnull String modelId, @Nonnull String name) {
        var machineLearning = applicationContext.getBean(name, AbstractMachineLearning.class);
        // TODO This can be expensive and will need to be enhanced later:
        var items = machineLearning.predict(modelId, jsonString, modelStorage).toJSON().collectAsList().stream()
                .map(Object::toString).collect(Collectors.joining(","));

        return "[" + items + "]";
    }

    @SuppressWarnings("unchecked")
    @Override
    public TrainingResponse trainModel(@Nonnull Map<String, Object> parameters, @Nonnull String name) {
        var machineLearning = applicationContext.getBean(name, AbstractMachineLearning.class);
        var dataset = machineLearning.buildDataset(parameters);

        var modelId = machineLearning.buildModel(dataset, parameters, modelStorage);
        var modelUri = modelStorage.getUriOfModel(modelId);

        return new TrainingResponse().modelId(modelId).modelUri(modelUri.toString());
    }
}
