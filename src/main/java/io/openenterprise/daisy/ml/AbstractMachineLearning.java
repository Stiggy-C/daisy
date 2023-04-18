package io.openenterprise.daisy.ml;

import com.amazonaws.services.s3.AmazonS3;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.awspring.cloud.core.io.s3.SimpleStorageResource;
import io.openenterprise.daisy.AbstractApplication;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.lang.reflect.MethodUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.shaded.com.google.common.collect.Lists;
import org.apache.spark.ml.util.MLWritable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.task.SyncTaskExecutor;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public abstract class AbstractMachineLearning<M extends MLWritable> extends AbstractApplication {

    @Inject
    protected AmazonS3 amazonS3;

    protected Class<M> modelClass;

    @Inject
    protected ObjectMapper objectMapper;

    @Value("${daisy.s3.bucket}")
    protected String s3Bucket;

    protected AbstractMachineLearning(@Nonnull Class<M> modelClass) {
        this.modelClass = modelClass;
    }

    @Nonnull
    public String buildAndStoreModel(@Nonnull Dataset<Row> dataset, @Nonnull Map<String, ?> parameters) throws IOException {
        var model = buildModel(dataset, parameters);
        var uuid = UUID.randomUUID();
        var s3Uri = getS3UriOfModel(uuid.toString());

        model.write().overwrite().save(StringUtils.replace(s3Uri.toString(), "s3", "s3a"));

        return uuid.toString();
    }

    @Nonnull
    public Dataset<Row> predict(@Nonnull String modelId, @Nonnull String jsonString)
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        var s3Uri = getS3UriOfModel(modelId);

        @SuppressWarnings("unchecked")
        var model = (M) MethodUtils.invokeStaticMethod(modelClass, "load",
                StringUtils.replace(s3Uri.toString(), "s3", "s3a"));

        return predict(model, jsonString);
    }

    @Nonnull
    protected abstract M buildModel(@Nonnull Dataset<Row> dataset, @Nonnull Map<String, ?> parameters);

    @Nonnull
    protected URI getS3UriOfModel(@Nonnull String modelId) {
        var s3path = "ml/models/" + modelId;
        var s3Resource = new SimpleStorageResource(amazonS3, s3Bucket, s3path, new SyncTaskExecutor());

        return s3Resource.getS3Uri();
    }

    @Nonnull
    protected abstract Dataset<Row> predict(@Nonnull M model, @Nonnull String jsonString);

    @Nonnull
    protected Dataset<Row> toDataset(@Nonnull String jsonString) throws JsonProcessingException {
        var rootJsonNode = objectMapper.readTree(jsonString);
        var jsonNodes = (rootJsonNode.isArray())? IteratorUtils.toList(rootJsonNode.elements())
                : Lists.newArrayList(new JsonNode[] { rootJsonNode });

        return sparkSession.read().json(sparkSession.createDataset(jsonNodes.stream().map(JsonNode::toString)
                .collect(Collectors.toList()), Encoders.STRING()));
    }

    @PostConstruct
    protected void postConstruct() {
        if (!amazonS3.doesBucketExistV2(s3Bucket)) {
            amazonS3.createBucket(s3Bucket);
        }
    }
}
