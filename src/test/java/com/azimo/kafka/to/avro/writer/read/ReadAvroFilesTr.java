package com.azimo.kafka.to.avro.writer.read;

import com.azimo.kafka.to.avro.writer.config.Constants;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import java.util.List;

public class ReadAvroFilesTr extends PTransform<PBegin, PCollection<AvroRecord>> {
    public static final String AVRO_FILES_SUFFIX = Constants.AVRO_FILE_NAME_PREFIX + "-*.avro";

    private List<String> paths;

    public ReadAvroFilesTr(List<String> paths) {
        this.paths = paths;
    }

    @Override
    public PCollection<AvroRecord> expand(PBegin p) {
        return p.apply(Create.of(paths)).setCoder(StringUtf8Coder.of())
                .apply(AvroIO.parseAllGenericRecords(g -> {
                    IndexedRecord record = SpecificData.get().deepCopy(g.getSchema(), g);
                    return AvroRecord.of(record);
                }).withCoder(SerializableCoder.of(AvroRecord.class)));
    }
}
