package leap.data.beam.transforms.aggregate;

import com.google.auto.value.AutoValue;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

public class LeapGroup {
    public static void byFieldNames(){

    }

    //@AutoValue
    public abstract static class ByFields extends PTransform<PCollection<GenericRecord>,PCollection<GenericRecord>>{

    }
}
