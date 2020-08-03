package leap.data.beam.transforms.join;

import com.google.auto.value.AutoValue;
import leap.data.beam.transforms.DeadLetterQueue;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public class WithDroppedJoinElements {

    @AutoValue
    public abstract static class Result<K, L, R>
            implements PInput, POutput {

        abstract PCollection<KV<K, KV<L, R>>> output();
        @Nullable
        abstract TupleTag<?> outputTag();

        abstract PCollection<L> leftDroppedElements();
        abstract TupleTag<L> leftDroppedElementsTag();

        abstract PCollection<R> rightDroppedElements();
        abstract TupleTag<R> rightDroppedElementsTag();

        abstract KvCoder<K, KV<L, R>> outputCoder();

        public static <K, L, R>
            WithDroppedJoinElements.Result<K, L, R> of(
                PCollection<KV<K, KV<L, R>>> output, PCollection<L> leftDroppedElements, PCollection<R> rightDroppedElements,
                KvCoder<K, KV<L, R>> outputCoder) {
            return new AutoValue_WithDroppedJoinElements_Result<>(
                    output, null, leftDroppedElements, new TupleTag<>(), rightDroppedElements, new TupleTag<>(),outputCoder);
        }

        public static <K, L, R>
        WithDroppedJoinElements.Result<K, L, R> of(
                PCollectionTuple tuple,
                TupleTag<KV<K, KV<L, R>>> outputTag,
                TupleTag<L> leftDroppedElementsTag,
                TupleTag<R> rightDroppedElementsTag,
                KvCoder<K, KV<L, R>> outputCoder) {
            return new AutoValue_WithDroppedJoinElements_Result<>(
                    tuple.get(outputTag), outputTag,
                    tuple.get(leftDroppedElementsTag), leftDroppedElementsTag,
                    tuple.get(rightDroppedElementsTag), rightDroppedElementsTag,
                    outputCoder);
        }

        /** Adds the dropped collection to the passed list and returns just the output collection. */
        public PCollection<KV<K, KV<L, R>>> droppedElementsTo(List<PCollection<L>> leftDroppedCollections,
                                         List<PCollection<R>> rightDroppedCollections) {
            if(leftDroppedCollections != null)
                leftDroppedCollections.add(leftDroppedElements());
            if(rightDroppedCollections != null)
                rightDroppedCollections.add(rightDroppedElements());

            return output();
        }

        public PCollection<KV<K, KV<L, R>>> droppedLeftElementsTo(List<PCollection<L>> leftDroppedCollections) {
            return droppedElementsTo(leftDroppedCollections,null);
        }

        public PCollection<KV<K, KV<L, R>>> droppedRightElementsTo(List<PCollection<R>> rightDroppedCollections) {
            return droppedElementsTo(null,rightDroppedCollections);
        }

        /** Returns just the output collection ignoring the dropped tuple. */
        public PCollection<KV<K, KV<L, R>>> droppedElementsIgnored() {
            return output();
        }

        /**
         *  Adds the dropped elemet collection to a configured dead letter queue and returns just the output collection.
         */
        public PCollection<KV<K, KV<L, R>>> droppedElementsToDeadLetter(DeadLetterQueue.DeadLetterFn<Void,L> leftDroppedElementDeadLetterFn,
                                            DeadLetterQueue.DeadLetterFn<Void,R> rightDroppedElementDeadLetterFn) {
            if(leftDroppedElementDeadLetterFn != null)
                leftDroppedElements()
                        .apply(WithKeys.of((SerializableFunction<L, Void>) input -> null))
                        .apply(leftDroppedElementDeadLetterFn);
            if(rightDroppedElementDeadLetterFn != null)
                rightDroppedElements()
                        .apply(WithKeys.of((SerializableFunction<R, Void>) input -> null))
                        .apply(rightDroppedElementDeadLetterFn);
            return output();
        }

        public PCollection<KV<K, KV<L, R>>> droppedLeftElementsToDeadLetter(DeadLetterQueue.DeadLetterFn<Void,L> leftDroppedElementDeadLetterFn) {
            return droppedElementsToDeadLetter(leftDroppedElementDeadLetterFn,null);
        }

        public PCollection<KV<K, KV<L, R>>> droppedRightElementsToDeadLetter(DeadLetterQueue.DeadLetterFn<Void,R> rightDroppedElementDeadLetterFn) {
            return droppedElementsToDeadLetter(null,rightDroppedElementDeadLetterFn);
        }

        @Override
        public Pipeline getPipeline() {
            return output().getPipeline();
        }

        @Override
        public Map<TupleTag<?>, PValue> expand() {
            Map<TupleTag<?>, PValue> values = new HashMap<>();
            KvCoder<L,R> valueCoder = (KvCoder<L,R>)outputCoder().getValueCoder();
            values.put(leftDroppedElementsTag(), leftDroppedElements().setCoder(valueCoder.getKeyCoder()));
            values.put(rightDroppedElementsTag(), rightDroppedElements().setCoder(valueCoder.getValueCoder()));
            if (outputTag() != null && output() != null) {
                values.put(outputTag(), output().setCoder(outputCoder()));
            }
            return values;
        }

        @Override
        public void finishSpecifyingOutput(
                String transformName, PInput input, PTransform<?, ?> transform) {}

    }
}

