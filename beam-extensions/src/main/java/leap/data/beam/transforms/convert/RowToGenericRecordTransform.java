package leap.data.beam.transforms.convert;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class RowToGenericRecordTransform extends PTransform<PCollection<Row>, PCollection<GenericRecord>> {

    public static RowToGenericRecordTransform convert(){
        return new RowToGenericRecordTransform();
    }

//    private final org.apache.avro.Schema schema;
//
//    public RowToGenericRecordTransform(org.apache.avro.Schema schema){
//        this.schema = schema;
//    }

    @Override
    public PCollection<GenericRecord> expand(PCollection<Row> input) {
        org.apache.avro.Schema schema = AvroUtils.toAvroSchema(input.getSchema());
        SerializableFunction<Row,GenericRecord> converter = AvroUtils.getRowToGenericRecordFunction(schema);
        return input.apply("ToRows", ParDo.of(new DoFn<Row, GenericRecord>() {
            @ProcessElement
            public void processElement(ProcessContext context) {
                context.output(converter.apply(context.element()));
            }
        })).setCoder(AvroCoder.of(schema));
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
