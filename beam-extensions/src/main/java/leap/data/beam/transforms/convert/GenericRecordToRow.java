package leap.data.beam.transforms.convert;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class GenericRecordToRow extends PTransform<PCollection<GenericRecord>, PCollection<Row>> {

    public static GenericRecordToRow convert(org.apache.avro.Schema schema){
        return new GenericRecordToRow(schema);
    }

    private final org.apache.avro.Schema schema;

    public GenericRecordToRow(org.apache.avro.Schema schema){
        this.schema = schema;
    }

    @Override
    public PCollection<Row> expand(PCollection<GenericRecord> input) {
        Schema beamSchema = AvroUtils.toBeamSchema(schema);
        SerializableFunction<GenericRecord,Row> converter = AvroUtils.getGenericRecordToRowFunction(beamSchema);
        return input.apply("ToRows", ParDo.of(new DoFn<GenericRecord, Row>() {
            @ProcessElement
            public void processElement(ProcessContext context) {
                context.output(converter.apply(context.element()));
            }
        })).setRowSchema(beamSchema);
    }
}
