package leap.data.beam.util;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;

public class AvroSchemaUtil {
    public static Schema mergeSchemas(Schema left, Schema right){
        Schema combinedRecord = Schema.createRecord(
                coalesce(left.getName(), right.getName()),
                coalesce(left.getDoc(), right.getDoc()),
                coalesce(left.getNamespace(), right.getNamespace()),
                false
        );
        combinedRecord.setFields(mergeFields(left, right));

        return combinedRecord;
    }

    private static List<Schema.Field> mergeFields(Schema left, Schema right) {
        List<Schema.Field> fields = new ArrayList<>();
        for (Schema.Field leftField : left.getFields()) {
            fields.add(copy(leftField));
        }

        for (Schema.Field rightField : right.getFields()) {
            if (left.getField(rightField.name()) == null) {
                fields.add(copy(rightField));
            }
        }

        return fields;
    }

    private static Schema.Field copy(Schema.Field field) {
        return new Schema.Field(
                field.name(), field.schema(), field.doc());
    }

    @SafeVarargs
    private static <E> E coalesce(E... objects) {
        for (E object : objects) {
            if (object != null) {
                return object;
            }
        }
        return null;
    }
}
