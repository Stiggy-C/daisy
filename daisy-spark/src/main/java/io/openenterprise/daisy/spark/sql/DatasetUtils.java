package io.openenterprise.daisy.spark.sql;

import com.fasterxml.jackson.databind.JsonNode;
import io.openenterprise.daisy.Invocation;
import io.openenterprise.daisy.InvocationContext;
import io.openenterprise.daisy.springframework.context.support.ApplicationContextUtils;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.lang3.reflect.TypeUtils;
import org.apache.hadoop.shaded.com.google.common.collect.Lists;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.stream.Collectors;

public final class DatasetUtils {

    private DatasetUtils() {}

    @SuppressWarnings("unchecked")
    @Nullable
    public static Dataset<Row> getDataset(@Nonnull InvocationContext invocationContext) {
        return (Dataset<Row>) invocationContext.getPastInvocations().stream()
                .map(Invocation::getResult)
                .filter(Objects::nonNull)
                .filter(object -> TypeUtils.isAssignable(object.getClass(), Dataset.class))
                .reduce((first, second) -> second)
                .orElse(null);
    }

    @Nonnull
    public static Dataset<Row> createVectorColumn(
            @Nonnull Dataset<Row> dataset, @Nonnull String vectorColumnName, @Nonnull String... columns) {
        VectorAssembler vectorAssembler = new VectorAssembler().setInputCols(columns).setOutputCol(vectorColumnName);

        return vectorAssembler.transform(dataset);
    }

    @Nonnull
    public static Dataset<Row> toDataset(@Nonnull JsonNode jsonNode) {
        var jsonNodes = (jsonNode.isArray())? IteratorUtils.toList(jsonNode.elements())
                : Lists.newArrayList(new JsonNode[] { jsonNode });

        var applicationContext = ApplicationContextUtils.getApplicationContext();
        var sparkSession = applicationContext.getBean(SparkSession.class);

        return sparkSession.read().json(sparkSession.createDataset(jsonNodes.stream().map(JsonNode::toString)
                .collect(Collectors.toList()), Encoders.STRING()));
    }
}
