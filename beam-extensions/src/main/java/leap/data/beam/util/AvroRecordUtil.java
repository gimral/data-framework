package leap.data.beam.util;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

public class AvroRecordUtil {
    public static GenericRecord mergeRecords(GenericRecord leftRecord, GenericRecord rightRecord){
        return mergeRecords(leftRecord,rightRecord,AvroSchemaUtil.mergeSchemas(leftRecord.getSchema(),rightRecord.getSchema()));
    }
    public static GenericRecord mergeRecords(GenericRecord leftRecord, GenericRecord rightRecord, Schema targetSchema){
        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(targetSchema);

        for (Schema.Field field :
                targetSchema.getFields()) {
            if (leftRecord.getSchema().getField(field.name()) != null) {
                recordBuilder.set(field.name(),leftRecord.get(field.name()));
            } else if (rightRecord.getSchema().getField(field.name()) != null) {
                recordBuilder.set(field.name(),rightRecord.get(field.name()));
            }
        }

        return recordBuilder.build();
    }
}
