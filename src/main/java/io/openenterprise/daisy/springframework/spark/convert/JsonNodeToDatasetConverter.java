package io.openenterprise.daisy.springframework.spark.convert;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.hadoop.shaded.com.google.common.collect.Lists;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

@Component
public class JsonNodeToDatasetConverter implements Converter<JsonNode, Dataset<Row>> {

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private SparkSession sparkSession;

    @Override
    public Dataset<Row> convert(@Nonnull JsonNode source) {
        return toDataset(source);
    }

    /**
     * Convert the object(s) in {@link JsonNode} to a {@link Dataset}.
     *
     * @param jsonNode
     * @return
     */
    @Nonnull
    protected Dataset<Row> toDataset(@Nonnull JsonNode jsonNode) {
        var jsonNodes = (jsonNode.isArray())? IteratorUtils.toList(jsonNode.elements())
                : Lists.newArrayList(new JsonNode[] { jsonNode });

        return sparkSession.read().json(sparkSession.createDataset(jsonNodes.stream().map(JsonNode::toString)
                .collect(Collectors.toList()), Encoders.STRING()));
    }
}
