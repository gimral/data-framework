package leap.data.beam.transforms;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public class WithInvalids {

    @AutoValue
    public abstract static class InvalidElement<T> implements Serializable {
        public abstract T element();

        @Nullable public abstract Exception exception();

        public static <T> WithInvalids.InvalidElement<T> of(T element) {
            return new AutoValue_WithInvalids_InvalidElement<>(element, null);
        }

        public static <T> WithInvalids.InvalidElement<T> of(T element, Exception exception) {
            return new AutoValue_WithInvalids_InvalidElement<>(element, exception);
        }
    }

//    public static class InvalidAsMapHandler<T>
//            extends SimpleFunction<WithInvalids.InvalidElement<T>, KV<T, Map<String, String>>> {
//        @Override
//        public KV<T, Map<String, String>> apply(WithInvalids.InvalidElement<T> f) {
//            return KV.of(
//                    f.element(),
//                    ImmutableMap.of(
//                            "className", f.exception().getClass().getName(),
//                            "message", f.exception().getMessage(),
//                            "stackTrace", Arrays.toString(f.exception().getStackTrace())));
//        }
//    }

    @AutoValue
    public abstract static class Result<OutputT extends POutput, InvalidElementT>
            implements PInput, POutput {

        public abstract OutputT output();

        @Nullable
        abstract TupleTag<?> outputTag();

        public abstract PCollection<InvalidElementT> invalids();

        abstract TupleTag<InvalidElementT> invalidsTag();

        public static <OutputT extends POutput, InvalidElementT> WithInvalids.Result<OutputT, InvalidElementT> of(
                OutputT output, PCollection<InvalidElementT> invalids) {
            return new AutoValue_WithInvalids_Result<>(
                    output, null, invalids, new TupleTag<>());
        }

        public static <OutputElementT, InvalidElementT>
        WithInvalids.Result<PCollection<OutputElementT>, InvalidElementT> of(
                PCollection<OutputElementT> output, PCollection<InvalidElementT> invalids) {
            return new AutoValue_WithInvalids_Result<>(
                    output, new TupleTag<OutputElementT>(), invalids, new TupleTag<>());
        }

        public static <OutputElementT, InvalidElementT>
        WithInvalids.Result<PCollection<OutputElementT>, InvalidElementT> of(
                PCollectionTuple tuple,
                TupleTag<OutputElementT> outputTag,
                TupleTag<InvalidElementT> invalidTag) {
            return new AutoValue_WithInvalids_Result<>(
                    tuple.get(outputTag), outputTag, tuple.get(invalidTag), invalidTag);
        }

        /** Adds the invalid collection to the passed list and returns just the output collection. */
        public OutputT invalidsTo(List<PCollection<InvalidElementT>> invalidCollections) {
            invalidCollections.add(invalids());
            return output();
        }

        public OutputT invalidsIgnored() {
            //TODO: Log
            return output();
        }

        public OutputT invalidsToDeadLetter(String topic, String bootstrapServers) {
            //TODO: DeadLetter DoFn builder
            invalids().apply(
                    DeadLetterQueue.<InvalidElementT>of(topic)
                            .withBootstrapServers(bootstrapServers));
            return output();
        }

        public OutputT invalidsToDeadLetter(DeadLetterQueue.DeadLetterFn<InvalidElementT> deadLetterFn) {
            //TODO: DeadLetter DoFn builder
            invalids().apply(deadLetterFn);
            return output();
        }

        @Override
        public Pipeline getPipeline() {
            return output().getPipeline();
        }

        @Override
        public Map<TupleTag<?>, PValue> expand() {
            Map<TupleTag<?>, PValue> values = new HashMap<>();
            values.put(invalidsTag(), invalids());
            if (outputTag() != null && output() instanceof PValue) {
                values.put(outputTag(), (PValue) output());
            }
            return values;
        }

        @Override
        public void finishSpecifyingOutput(
                String transformName, PInput input, PTransform<?, ?> transform) {}
    }
}
