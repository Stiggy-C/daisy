package io.openenterprise.daisy.spark.sql.streaming.service;

import com.google.common.collect.Maps;
import io.openenterprise.daisy.Parameter;
import io.openenterprise.daisy.spark.sql.service.AbstractBaseDatasetServiceImpl;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static io.openenterprise.daisy.spark.sql.Parameter.*;
import static io.openenterprise.daisy.spark.sql.streaming.Parameter.CHECKPOINT_LOCATION;
import static io.openenterprise.daisy.spark.sql.streaming.Parameter.OUTPUT_MODE;

@Service("streamingDatasetService")
public class StreamingDatasetServiceImpl extends AbstractBaseDatasetServiceImpl<StreamingQuery>
        implements StreamingDatasetService {

    @Autowired
    protected ApplicationContext applicationContext;

    @Value("${daisy.spark.streaming.checkpoint-location}")
    protected String defaultCheckpointLocation;

    @Nullable
    @SuppressWarnings("unchecked")
    @Override
    public Dataset<Row> loadDataset(@Nonnull Map<String, Object> parameters) {
        Dataset<Row> dataset = null;

        verifyParametersForLoadingDataset(parameters);

        if (parameters.containsKey(DATASET.getKey())) {
            dataset = (Dataset<Row>) MapUtils.getObject(parameters, DATASET.getKey());

            if (!dataset.isStreaming()) {
                throw new UnsupportedOperationException("Dataset is not streaming");
            }
        } else if (parameters.containsKey(DATASET_FORMAT.getKey())) {
            var format = getFormat(parameters);

            if (!getStreamingCapableFormats().contains(format)) {
                throw new UnsupportedOperationException(format + " format does not support streaming");
            }

            var options = getOptions(parameters);
            var path = getPath(parameters);

            dataset = loadDataset(format, options, path);
        }  else if (parameters.containsKey(DATASET_TABLE.getKey())) {
            dataset = loadDataset(MapUtils.getString(parameters, DATASET_TABLE.getKey()));
        } else if (parameters.containsKey(DATASET_VIEW.getKey())) {
            dataset = loadDataset(MapUtils.getString(parameters, DATASET_VIEW.getKey()));
        } else {
            throw new IllegalStateException();
        }

        return dataset;
    }

    @Nonnull
    @Override
    public Dataset<Row> loadDataset(@Nonnull String format, @Nullable Map<String, String> options, @Nonnull String path) {
        return sparkSession.readStream().format(format).options(Objects.isNull(options)? Maps.newHashMap() : options)
                .load(path);
    }

    @Nonnull
    @Override
    public Dataset<Row> loadDataset(@Nonnull String tableOrView) {
        return sparkSession.readStream().table(tableOrView);
    }

    @Override
    public StreamingQuery saveDataset(@Nonnull Dataset<Row> dataset, @Nonnull Map<String, Object> parameters) throws Exception {
        var checkpointLocation = parameters.containsKey(CHECKPOINT_LOCATION.getKey())?
                parameters.get(CHECKPOINT_LOCATION.getKey()).toString() :
                defaultCheckpointLocation + "/" + parameters.get(Parameter.SESSION_ID.getKey());
        var outputMode = parameters.containsKey(OUTPUT_MODE.getKey())?
                determineOutputMode(MapUtils.getObject(parameters, OUTPUT_MODE.getKey())) : OutputMode.Append();

        var options = Maps.newHashMap(Map.of("checkpointLocation", checkpointLocation));

        StreamingQuery streamingQuery;

        if (parameters.containsKey(DATASET_FORMAT.getKey())) {
            var format = getFormat(parameters);
            var formatOptions = getOptions(parameters);

            options.putAll(formatOptions);

            if (!getStreamingCapableFormats().contains(format)) {
                throw new UnsupportedOperationException(format + " does not support streaming");
            }

            if (parameters.containsKey(DATASET_PATH.getKey())) {
                var path = MapUtils.getString(parameters, DATASET_PATH.getKey());

                streamingQuery = saveDatasetExternally(dataset, path, format, options, outputMode);
            } else if (parameters.containsKey(DATASET_TABLE.getKey())) {
                var table = MapUtils.getString(parameters, DATASET_TABLE.getKey());

                streamingQuery = saveDataset(dataset, table, format, options, outputMode);
            } else {
                throw new UnsupportedOperationException();
            }
        } else if (parameters.containsKey(DATASET_TABLE.getKey())) {
            var table = MapUtils.getString(parameters, DATASET_TABLE.getKey());

            streamingQuery = saveDataset(dataset, table, null, options, outputMode);
        } else {
            throw new UnsupportedOperationException();
        }

        return streamingQuery;
    }

    @Nonnull
    @Override
    public StreamingQuery saveDataset(@Nonnull Dataset<Row> dataset, @Nonnull String table, @Nullable String format,
                                      @Nullable Map<String, String> options, @Nullable OutputMode outputMode)
            throws TimeoutException {
        return getDataStreamWriter(dataset, format, options, outputMode).toTable(table);
    }

    @Nonnull
    @Override
    public StreamingQuery saveDatasetExternally(@Nonnull Dataset<Row> dataset, @Nonnull String path,
                                                @Nullable String format,
                                                @Nullable Map<String, String> options,
                                                @Nullable OutputMode outputMode) {
        return getDataStreamWriter(dataset, format, options, outputMode).start(path);
    }

    @Nonnull
    protected DataStreamWriter<Row> getDataStreamWriter(@Nonnull Dataset<Row> dataset, @Nullable String format,
                                                        @Nullable Map<String, String> options,
                                                        @Nullable OutputMode outputMode) {
        var dataStreamWriter = dataset.writeStream();

        if (StringUtils.isNotEmpty(format)) {
            dataStreamWriter = dataStreamWriter.format(format);
        }

        if (Objects.nonNull(outputMode)) {
            dataStreamWriter = dataStreamWriter.outputMode(outputMode);
        }

        return dataStreamWriter.options(Objects.isNull(options)? Maps.newHashMap() : options);
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    protected final Set<String> getStreamingCapableFormats() {
        return applicationContext.getBean("streamingCapableFormats", Set.class);
    }

    @Nonnull
    protected OutputMode determineOutputMode(@Nullable Object object) {
        if (Objects.isNull(object)) {
            return OutputMode.Append();
        }

        if (ClassUtils.isAssignable(object.getClass(), OutputMode.class)) {
            return (OutputMode) object;
        } else if (ClassUtils.isAssignable(object.getClass(), String.class)) {
            var string = StringUtils.lowerCase(object.toString());
            OutputMode outputMode;

            switch (string) {
                case "complete":
                    outputMode = OutputMode.Complete();
                    break;
                case "update":
                    outputMode =  OutputMode.Update();
                    break;
                default:
                    outputMode =  OutputMode.Append();
            }

            return outputMode;
        } else {
            throw new UnsupportedOperationException("Unable to determine OutputMode");
        }
    }
}
