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

public class GenericRecordToRowTransform extends PTransform<PCollection<GenericRecord>, PCollection<Row>> {

    public static GenericRecordToRowTransform convert(org.apache.avro.Schema schema){
        return new GenericRecordToRowTransform(schema);
    }

    private final org.apache.avro.Schema schema;

    public GenericRecordToRowTransform(org.apache.avro.Schema schema){
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

//    private static class GenericRecordToRowDoFn extends DoFn<GenericRecord, Row>{
//        public GenericRecordToRowDoFn(){
//
//        }
//        @ProcessElement
//        public void processElement(ProcessContext context) {
//            context.output(AvroUtils.toBeamRowStrict(context.element(), null));
//        }
//    }
}
