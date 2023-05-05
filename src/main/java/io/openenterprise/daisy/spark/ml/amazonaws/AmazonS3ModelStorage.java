package io.openenterprise.daisy.spark.ml.amazonaws;

import com.amazonaws.services.s3.AmazonS3;
import io.awspring.cloud.core.io.s3.SimpleStorageResource;
import io.openenterprise.daisy.spark.ml.ModelStorage;
import lombok.SneakyThrows;
import org.apache.commons.lang.reflect.MethodUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.util.MLWritable;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.task.SyncTaskExecutor;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;
import java.net.URI;

@Named
public class AmazonS3ModelStorage implements ModelStorage {

    @Inject
    protected AmazonS3 amazonS3;

    @Value("${daisy.spark.model.directory:/ml/models}")
    private String directory;

    @Value("${daisy.s3.bucket}")
    protected String s3Bucket;



    /**
     * Get the S3 URI of the model which has the given modelId.
     *
     * @param modelId
     * @return
     */
    @Nonnull
    @Override
    public URI getUriOfModel(@Nonnull String modelId) {
        var s3path = (StringUtils.startsWith(directory, "/")?
                StringUtils.replaceOnce(directory, "/", "") : directory) + "/" + modelId;
        var s3Resource = new SimpleStorageResource(amazonS3, s3Bucket, s3path, new SyncTaskExecutor());

        return s3Resource.getS3Uri();
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    @Override
    public <M extends Model<M> & MLWritable> M load(@Nonnull Class<M> modelClass, @NotNull String uid) {
        var s3Uri = getUriOfModel(uid);

        return (M) MethodUtils.invokeStaticMethod(modelClass, "load", StringUtils.replace(
                s3Uri.toString(), "s3", "s3a"));
    }


    @SneakyThrows
    @Override
    public <M extends Model<M> & MLWritable> URI store(@NotNull M model) {
        var s3Uri = getUriOfModel(model.uid());

        model.write().overwrite().save(StringUtils.replace(s3Uri.toString(), "s3", "s3a"));

        return s3Uri;
    }

    @PostConstruct
    protected void postConstruct() {
        if (!amazonS3.doesBucketExistV2(s3Bucket)) {
            amazonS3.createBucket(s3Bucket);
        }
    }
}
