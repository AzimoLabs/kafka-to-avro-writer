package com.azimo.kafka.to.avro.writer;

import com.azimo.kafka.to.avro.writer.config.ConfigUtils;
import com.azimo.kafka.to.avro.writer.config.Constants;
import com.azimo.kafka.to.avro.writer.config.KafkaToAvroWriterPipelineOptions;
import com.azimo.kafka.to.avro.writer.read.ReadKafkaGenericTr;
import com.azimo.kafka.to.avro.writer.serialize.AvroGenericCoder;
import com.azimo.kafka.to.avro.writer.serialize.AvroGenericRecord;
import com.azimo.kafka.to.avro.writer.write.WriteAvroFilesTr;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class KafkaToAvroWriterApp {

    public static void main(String[] args) {
        KafkaToAvroWriterPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(KafkaToAvroWriterPipelineOptions.class);
        new KafkaToAvroWriterApp().run(options);
    }

    public void run(KafkaToAvroWriterPipelineOptions options) {
        configureStaticOptions(options);
        Pipeline p = Pipeline.create(options);

        ReadKafkaGenericTr readKafkaTr = ReadKafkaGenericTr.newReadKafkaGenericTrBuilder().inputTopics(ConfigUtils.convertCsvToList(options.getInputTopics(), Constants.TOPIC_SEPARATOR))
                .bootstrapServers(options.getBootstrapServers())
                .schemaRegistryUrl(options.getSchemaRegistryUrl())
                .offsetReset(options.getOffsetReset())
                .consumerGroupId(options.getConsumerGroupId())
                .isEnableAutoCommit(options.isEnableAutoCommit())
                .build();

                PCollection<AvroGenericRecord> records = p.apply(readKafkaTr)
                .apply(Window.<AvroGenericRecord>into(FixedWindows.of(Duration.standardMinutes(options.getWindowInMinutes())))
                        .triggering(AfterWatermark.pastEndOfWindow()
                                .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
                                        .plusDelayOf(Duration.standardMinutes(options.getWindowInMinutes())))
                                .withLateFirings(AfterPane.elementCountAtLeast(1)))
                        .withAllowedLateness(Duration.standardDays(2))
                        .discardingFiredPanes()
                    ).setCoder(AvroGenericCoder.of(options.getSchemaRegistryUrl()));

        records.apply(new WriteAvroFilesTr(options.getBasePath(), options.getNumberOfShards()));

        p.run();
    }

    private void configureStaticOptions(KafkaToAvroWriterPipelineOptions options) {
        options.setAppName(Constants.APP_NAME);
    }
}
