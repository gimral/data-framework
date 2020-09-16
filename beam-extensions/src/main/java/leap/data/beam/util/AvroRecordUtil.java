package leap.data.beam.util;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class AvroRecordUtil {
    public static GenericRecord mergeRecords(GenericRecord leftRecord, GenericRecord rightRecord, List<MergeMapping> mappings){
        return mergeRecords(leftRecord,rightRecord,
                AvroSchemaUtil.mergeSchemas(leftRecord.getSchema(),rightRecord.getSchema()),mappings);
    }
    public static GenericRecord mergeRecords(GenericRecord leftRecord, GenericRecord rightRecord, Schema targetSchema,
                                             List<MergeMapping> mappings){
        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(targetSchema);
        for (MergeMapping mapping :
                mappings) {
            recordBuilder.set(mapping.getTargetFieldName(),mapping.getMergeMappingSide() == MergeMappingSide.LEFT ?
                    leftRecord.get(mapping.getSourceFieldName()) :
                    rightRecord.get(mapping.getTargetFieldName()));
        }
        return recordBuilder.build();
    }

    public static List<MergeMapping> getMergeMapping(Schema leftSchema, Schema rightSchema, Schema targetSchema,
                                       String leftPrefix, String rightPrefix){
        List<MergeMapping> mappings = new ArrayList<>();
        for (Schema.Field field :
                targetSchema.getFields()) {
            if (leftSchema.getField(field.name()) != null) {
                mappings.add(new MergeMapping(MergeMappingSide.LEFT,field.name(),field.name()));
            } else if (rightSchema.getField(field.name()) != null) {
                mappings.add(new MergeMapping(MergeMappingSide.RIGHT,field.name(),field.name()));
            } else if (leftPrefix != null && !leftPrefix.isEmpty()
                    && leftSchema.getField(leftPrefix + field.name()) != null) {
                mappings.add(new MergeMapping(MergeMappingSide.LEFT,field.name(),leftPrefix + field.name()));
            } else if (rightPrefix != null && !rightPrefix.isEmpty()
                    && leftSchema.getField(rightPrefix + field.name()) != null) {
                mappings.add(new MergeMapping(MergeMappingSide.RIGHT,field.name(),rightPrefix + field.name()));
            }
        }
        return mappings;
    }

    public enum MergeMappingSide{
        LEFT,
        RIGHT
    }

    public static class MergeMapping implements Serializable {
        private MergeMappingSide mergeMappingSide;
        private String sourceFieldName;
        private String targetFieldName;

        public MergeMapping(MergeMappingSide mergeMappingSide, String sourceFieldName, String targetFieldName) {
            this.mergeMappingSide = mergeMappingSide;
            this.sourceFieldName = sourceFieldName;
            this.targetFieldName = targetFieldName;
        }

        public MergeMappingSide getMergeMappingSide() {
            return mergeMappingSide;
        }

        public String getSourceFieldName() {
            return sourceFieldName;
        }

        public String getTargetFieldName() {
            return targetFieldName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MergeMapping that = (MergeMapping) o;
            return mergeMappingSide == that.mergeMappingSide &&
                    sourceFieldName.equals(that.sourceFieldName) &&
                    targetFieldName.equals(that.targetFieldName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(mergeMappingSide, sourceFieldName, targetFieldName);
        }
    }
}
