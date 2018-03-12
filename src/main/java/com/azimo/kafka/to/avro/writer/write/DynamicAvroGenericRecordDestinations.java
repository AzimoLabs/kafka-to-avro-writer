package com.azimo.kafka.to.avro.writer.write;

import com.azimo.kafka.to.avro.writer.config.Constants;
import com.azimo.kafka.to.avro.writer.serialize.AvroDestination;
import com.azimo.kafka.to.avro.writer.serialize.AvroGenericRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.DynamicAvroDestinations;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import static com.google.common.base.MoreObjects.firstNonNull;
import static org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY;
import static org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions.RESOLVE_FILE;

public class DynamicAvroGenericRecordDestinations extends DynamicAvroDestinations<AvroGenericRecord, AvroDestination, GenericRecord> {
    private final String baseDir;
    private final String fileExtension;

    public DynamicAvroGenericRecordDestinations(String baseDir, String fileExtension) {
        this.baseDir = baseDir;
        this.fileExtension = fileExtension;
    }

    @Override
    public Schema getSchema(AvroDestination destination) {
        return new Schema.Parser().parse(destination.jsonSchema);
    }

    @Override
    public GenericRecord formatRecord(AvroGenericRecord record) {
        return record.record;
    }

    @Override
    public AvroDestination getDestination(AvroGenericRecord record) {
        Schema schema = record.record.getSchema();
        String name = schema.getName();
        return AvroDestination.of(name, schema.toString());
    }

    @Override
    public AvroDestination getDefaultDestination() {
        throw new RuntimeException("Default destination not supported");
    }

    @Override
    public FileBasedSink.FilenamePolicy getFilenamePolicy(AvroDestination destination) {
        String pathStr = baseDir + "/" + destination.name + Constants.AVRO_FILE_NAME_PREFIX;
        return new WindowedFilenamePolicy(FileBasedSink.convertToFileResourceIfPossible(pathStr), fileExtension);
    }

    private static class WindowedFilenamePolicy extends FileBasedSink.FilenamePolicy {
        final ResourceId outputFilePrefix;
        final String fileExtension;

        WindowedFilenamePolicy(ResourceId outputFilePrefix, String fileExtension) {
            this.outputFilePrefix = outputFilePrefix;
            this.fileExtension = fileExtension;
        }

        @Override
        public ResourceId windowedFilename(
                int shardNumber,
                int numShards,
                BoundedWindow window,
                PaneInfo paneInfo,
                FileBasedSink.OutputFileHints outputFileHints) {
            String filenamePrefix =
                    outputFilePrefix.isDirectory() ? "" : firstNonNull(outputFilePrefix.getFilename(), "");
            String filename =
                    String.format(
                            "%s-%s-%s-of-%s-pane-%s%s%s",
                            filenamePrefix,
                            window,
                            shardNumber,
                            numShards - 1,
                            paneInfo.getIndex(),
                            paneInfo.isLast() ? "-final" : "",
                            fileExtension);

            String subDir = null;
            //In case of interval windows we want to have daily sub directory.
            // In case of global windows we are skipping it. Global windows are only used in tests.
            if (window instanceof IntervalWindow) {
                subDir = getDateSubDir((IntervalWindow) window);
            }

            ResourceId result = outputFilePrefix.getCurrentDirectory();

            if (subDir != null)
                result = result.resolve(subDir, RESOLVE_DIRECTORY);
            return result.resolve(filename, RESOLVE_FILE);
        }

        private String getDateSubDir(IntervalWindow window) {
            Instant start = window.start();
            DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern(Constants.DATE_SUBDIR_FORMAT);
            return start.toString(dateTimeFormatter);
        }

        @Override
        public ResourceId unwindowedFilename(
                int shardNumber, int numShards, FileBasedSink.OutputFileHints outputFileHints) {
            throw new UnsupportedOperationException("Expecting windowed outputs only");
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            builder.add(
                    DisplayData.item("fileNamePrefix", outputFilePrefix.toString())
                            .withLabel("File Name Prefix"));
        }
    }

}
