package io.openenterprise.daisy.spark.ml;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openenterprise.daisy.spark.sql.AbstractSparkSqlService;
import io.openenterprise.daisy.springframework.spark.convert.JsonNodeToDatasetConverter;
import lombok.Getter;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.util.MLWritable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import javax.annotation.Nonnull;
import javax.cache.Cache;
import javax.inject.Inject;
import javax.inject.Named;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * This is the base of machine learning operations to be run on an Apache Spark cluster.
 *
 * @param <M>
 */
public abstract class AbstractMachineLearningService<M extends Transformer & MLWritable> extends AbstractSparkSqlService
        implements MachineLearning<M> {

    @Inject
    protected JsonNodeToDatasetConverter jsonNodeToDatasetConverter;

    @Inject
    @Named("sparkModelCache")
    protected Cache<String, Transformer> modelCache;

    @Getter
    protected Class<M> modelClass;

    @Inject
    protected ObjectMapper objectMapper;

    protected AbstractMachineLearningService(@Nonnull Class<M> modelClass) {
        this.modelClass = modelClass;
    }

    /**
     * Build/train the model against the given {@link Dataset}. Built/trained model will be cached to {@link Cache}
     * [which will be gone when daisy (or its JVM) terminated] and stored to the given {@link ModelStorage} to be used
     * later.
     *
     * @param dataset
     * @param parameters
     * @param modelStorage
     * @return
     */
    @Nonnull
    public String buildModel(
            @Nonnull Dataset<Row> dataset, @Nonnull Map<String, Object> parameters, @Nonnull ModelStorage modelStorage) {
        var model = buildModel(dataset, parameters);

        modelCache.put(model.uid(), model);
        modelStorage.store(model);

        return model.uid();
    }

    /**
     * Fetch the the model with the given id from {@link Cache} (or {@link ModelStorage} if model was evicted from cache).
     * After the model is fetch, make 1 or more predictions by making use of the model of the given modelId which will
     * be fetched from S3 bucket. Given input json can be one row or multiple rows of data. However, they must have the
     * same schema with the dataset which was used to build the given model.
     *
     * @param modelId
     * @param jsonString
     * @param modelStorage
     * @return
     * @throws InvocationTargetException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     */

    @SuppressWarnings("unchecked")
    @Nonnull
    public Dataset<Row> predict(@Nonnull String modelId, @Nonnull String jsonString, @Nonnull Map<String, ?> parameters,
                                @Nonnull ModelStorage modelStorage)
            throws JsonProcessingException {
        var model = modelCache.containsKey(modelId)? (M) modelCache.get(modelId)
                : modelStorage.load(modelClass, modelId);

        return predict(model, jsonString, parameters);
    }

    /**
     * Built/train the model with the given {@link Dataset}. Need to be filled in by the implementation.
     *
     * @param dataset
     * @param parameters
     * @return
     */
    @Nonnull
    protected abstract M buildModel(@Nonnull Dataset<Row> dataset, @Nonnull Map<String, ?> parameters);

    @Nonnull
    protected Dataset<Row> convertJsonStringToDataset(@Nonnull String jsonString) throws JsonProcessingException {
        var jsonNode = objectMapper.readTree(jsonString);

        return jsonNodeToDatasetConverter.convert(jsonNode);
    }
}
