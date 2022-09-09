package leap.data.beam.transforms.external;

import com.google.auto.value.AutoValue;
import leap.data.beam.transforms.WithInvalids;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

@AutoValue
@AutoValue.CopyAnnotations
public class IgniteEnrichment<T> extends PTransform<PCollection<T>,
        WithInvalids.Result<PCollection<T>, WithInvalids.InvalidElement<T>>> {

    @Override
    public WithInvalids.Result<PCollection<T>, WithInvalids.InvalidElement<T>> expand(PCollection<T> input) {
        return null;
    }

    private class IgniteEnrichmentDoFn extends DoFn<T, T> {
        @ProcessElement
        public void processElement(@Element T element, ProcessContext c){

        }
    }

}
