package com.azimo.kafka.to.avro.writer.write;

import com.azimo.kafka.to.avro.writer.config.Constants;
import com.azimo.kafka.to.avro.writer.serialize.AvroDestination;
import com.azimo.kafka.to.avro.writer.serialize.AvroGenericRecord;
import org.apache.avro.Schema;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.commons.codec.digest.DigestUtils;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.text.DecimalFormat;
import java.util.concurrent.ThreadLocalRandom;

public class WriteAvroFilesTr extends PTransform<PCollection<AvroGenericRecord>, POutput> {
    private String baseDir;
    private int numberOfShards;
    private String tempLocation;

    public WriteAvroFilesTr(String baseDir, String tempLocation, int numberOfShards) {
        this.baseDir = baseDir;
        this.numberOfShards = numberOfShards;
        this.tempLocation = tempLocation;
    }

    @Override
    public POutput expand(PCollection<AvroGenericRecord> input) {
        return input.apply("Write to avro files",
                FileIO.<AvroDestination, AvroGenericRecord>writeDynamic()
                        .by((record) -> {
                            Schema schema = record.record.getSchema();
                            String name = schema.getName();
                            return AvroDestination.of(name, schema.toString());
                        })
                        .via(Contextful.fn(record -> record.record),
                                Contextful.fn(d -> AvroIO.sink(d.jsonSchema)))
                        .withDestinationCoder(AvroCoder.of(AvroDestination.class))
                        .to(baseDir)
                        .withTempDirectory(tempLocation)
                        .withNumShards(numberOfShards)
                        .withNaming(DynamicAvroFileNaming::new));
    }

    class DynamicAvroFileNaming implements FileIO.Write.FileNaming {

        private AvroDestination avroDestination;

        public DynamicAvroFileNaming(AvroDestination avroDestination) {
            this.avroDestination = avroDestination;
        }

        @Override
        public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex, Compression compression) {
            String subDir = avroDestination.name;
            if (window instanceof IntervalWindow) {
                subDir += "/" + getDateSubDir((IntervalWindow) window);
            }
            String numShardsStr = String.valueOf(numShards);
            DecimalFormat df = new DecimalFormat("000000000000".substring(0, Math.max(5, numShardsStr.length())));
            String fileName = String.format(
                    "%s/%s-%s-[nanoTime=%s-random=%s]-%s-%s-of-%s-pane-%s%s%s",
                    subDir,
                    "events",
                    DigestUtils.md5Hex(avroDestination.jsonSchema),
                    System.nanoTime(),
                    ThreadLocalRandom.current().nextInt(100000),
                    window,
                    df.format(shardIndex),
                    df.format(numShards),
                    pane.getIndex(),
                    pane.isLast() ? "-final" : "",
                    Constants.FILE_EXTENSION
            );
            return fileName;
        }

        private String getDateSubDir(IntervalWindow window) {
            Instant maxTimestamp = window.start();
            DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern(Constants.DATE_SUBDIR_FORMAT);
            return Constants.DATE_SUBDIR_PREFIX + maxTimestamp.toString(dateTimeFormatter);
        }
    }
}
