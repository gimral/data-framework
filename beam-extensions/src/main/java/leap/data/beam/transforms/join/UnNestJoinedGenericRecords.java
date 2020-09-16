package leap.data.beam.transforms.join;

import leap.data.beam.util.AvroRecordUtil;
import leap.data.beam.util.AvroSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class UnNestJoinedGenericRecords<K>
        extends PTransform<PCollection<KV<K,KV<GenericRecord,GenericRecord>>>,PCollection<KV<K,GenericRecord>>> {

    private Schema writeSchema;
    public static <K> UnNestJoinedGenericRecords<K> of(){
        return new UnNestJoinedGenericRecords<>(null);
    }

    public static <K> UnNestJoinedGenericRecords<K> of(Schema writeSchema){
        return new UnNestJoinedGenericRecords<>(writeSchema);
    }

    public UnNestJoinedGenericRecords(Schema writeSchema){
        this.writeSchema = writeSchema;
    }

    @Override
    public PCollection<KV<K,GenericRecord>> expand(PCollection<KV<K, KV<GenericRecord, GenericRecord>>> input) {
        KvCoder<K, KV<GenericRecord, GenericRecord>> kvCoder = (KvCoder<K,
                KV<GenericRecord, GenericRecord>>) input.getCoder();
        if(writeSchema == null) {
            KvCoder<GenericRecord, GenericRecord> coder =
                    (KvCoder<GenericRecord, GenericRecord>) kvCoder.getValueCoder();
            @SuppressWarnings("unchecked")
            AvroCoder<GenericRecord> leftCoder = (AvroCoder<GenericRecord>) coder.getKeyCoder();
            @SuppressWarnings("unchecked")
            AvroCoder<GenericRecord> rightCoder = (AvroCoder<GenericRecord>) coder.getValueCoder();

            writeSchema = AvroSchemaUtil.mergeSchemas(leftCoder.getSchema(), rightCoder.getSchema());
        }
        return input.apply("UnNest Joined Records", ParDo.of(new UnNestJoinedGenericRecordsDoFn<>(writeSchema)))
                    .setCoder(KvCoder.of(kvCoder.getKeyCoder(),AvroCoder.of(writeSchema)));
    }


    private static class UnNestJoinedGenericRecordsDoFn<K>
            extends DoFn<KV<K,KV<GenericRecord,GenericRecord>>,KV<K,GenericRecord>>{

        private final Schema mergedSchema;

        private UnNestJoinedGenericRecordsDoFn(Schema mergedSchema) {
            this.mergedSchema = mergedSchema;
        }

        @ProcessElement
        public void processElement(ProcessContext c){
            K key = c.element().getKey();
            KV<GenericRecord,GenericRecord> values = c.element().getValue();
            c.output(KV.of(key, AvroRecordUtil.mergeRecords(values.getKey(),values.getValue(), mergedSchema)));
        }
    }
}
