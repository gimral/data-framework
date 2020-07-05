package leap.data.beam.transforms;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A collection of utilities for writing transforms that can handle invalid elements encountered during
 * processing of elements.
 *
 * <p>This handler is responsible for producing some output element that
 * captures relevant details of the invalid record and can be encoded as part of a invalid output {@link
 * PCollection}. Transforms can then package together their output and invalid collections in a
 * {@link WithInvalids.Result} that avoids users needing to interact with {@code TupleTag}s and
 * indexing into a {@link PCollectionTuple}.
 *
 *
 * <p>Users can take advantage of {@link Result#invalidsTo(List)} for fluent chaining of transforms
 * that handle invalids:
 *
 * <pre>{@code
 * PCollection<Integer> input = ...
 * List<PCollection<Map<String, String>> invalidCollections = new ArrayList<>();
 * input.apply(Validate.via(...))
 *      .invalidsTo(invalidCollections)
 *      .apply(Validate.via(...))
 *      .invalidsTo(invalidCollections);
 * PCollection<Map<String, String>> invalids = PCollectionList.of(invalidCollections)
 *      .apply("FlattenInvalidCollections", Flatten.pCollections());
 * }</pre>
 */
@SuppressWarnings("unused")
public class WithInvalids {

    /**
     * Exception type raised by Invalid output producers
     */
    public static class InvalidElementException extends Exception{
        public InvalidElementException(String message){
            super(message);
        }
        public InvalidElementException(String message, Throwable cause){
            super(message, cause);
        }
        public InvalidElementException(Throwable cause){
            super(cause);
        }
    }

    /**
     * The value type emitted by Invalid output producers. It wraps an invalid record exception together with the
     * input element that was being processed at the time the invalid output was raised.
     *
     */
    @AutoValue
    public abstract static class InvalidElement<T> implements Serializable {
        public abstract T element();

        @Nullable public abstract InvalidElementException exception();

        public static <T> WithInvalids.InvalidElement<T> of(T element) {
            return new AutoValue_WithInvalids_InvalidElement<>(element, null);
        }

        public static <T> WithInvalids.InvalidElement<T> of(T element, InvalidElementException exception) {
            return new AutoValue_WithInvalids_InvalidElement<>(element, exception);
        }
    }

    /**
     * An intermediate output type for PTransforms that allows an output collection to live alongside
     * a collection of invalid elements.
     *
     * @param <OutputT> Output type
     * @param <InvalidElementT> Element type for the invalid element {@code PCollection}
     */
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

        /** Returns just the output collection ignoring the invalid tuple. */
        public OutputT invalidsIgnored() {
            //TODO: Log
            return output();
        }

        /**
         *  Adds the invalid collection to a dead letter queue using the specified topic
         *  and returns just the output collection.
         */
//        public OutputT invalidsToDeadLetter(String topic) {
//            invalids().apply(
//                    DeadLetterQueue.<Void,InvalidElementT>of(topic));
//            return output();
//        }

        /**
         *  Adds the invalid collection to a configured dead letter queue and returns just the output collection.
         */
        public OutputT invalidsToDeadLetter(DeadLetterQueue.DeadLetterFn<Void,InvalidElementT> deadLetterFn) {
            invalids()
                    .apply(ParDo.of(new DoFn<InvalidElementT, KV<Void,InvalidElementT>>() {
                        @ProcessElement
                        public void processElement(@Element InvalidElementT element,  ProcessContext c) {
                            c.output(KV.of(null,element));
                        }
                    }))
                    .apply(deadLetterFn);
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
