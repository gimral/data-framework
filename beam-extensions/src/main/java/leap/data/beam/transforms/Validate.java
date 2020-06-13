package leap.data.beam.transforms;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import javax.annotation.Nullable;

/**
 * {@code PTransform}s for validating elements of a {@code PCollection} by satisfying a predicate.
 *
 *
 * <p>See {@link WithInvalids} documentation for usage patterns of the returned {@link
 * WithInvalids.Result}.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * Result<PCollection<String>, String>> result = words.apply(
 *     Validate
 *         .by((String word) -> word.length < 50) // Length of work should be less than 50 to be valid element
 *         .withExceptionsAsInvalid())  //if any exception occurs while validating, add element to invalids and ignore exception
 * PCollection<Integer> output = result.output();
 * PCollection<String> invalids = result.invalids();
 * }</pre>
 *
 *
 * @param <T> the type of the values in the input {@code PCollection}, and the type of the elements
 *     in the output {@code PCollection}
 */
public class Validate<T> extends PTransform<PCollection<T>,
        WithInvalids.Result<PCollection<T>, WithInvalids.InvalidElement<T>>> {

    private ProcessFunction<T, Boolean> predicate;
    private boolean exceptionsAsInvalid;

    public Validate(@Nullable ProcessFunction<T, Boolean> predicate, boolean exceptionsAsInvalid) {
        this.predicate = predicate;
        this.exceptionsAsInvalid = exceptionsAsInvalid;
    }

    public static <T, PredicateT extends ProcessFunction<T, Boolean>> Validate<T> by(
            PredicateT predicate) {
        return new Validate<>(predicate, false);
    }

    /** Binary compatibility adapter for {@link #by(ProcessFunction)}. */
    public static <T> Validate<T> by(
            SerializableFunction<T, Boolean> predicate) {
        return by((ProcessFunction<T, Boolean>) predicate);
    }

    public Validate<T> withExceptionsAsInvalid(){
        return new Validate<>(predicate, true);
    }

    @Override
    public WithInvalids.Result<PCollection<T>, WithInvalids.InvalidElement<T>> expand(PCollection<T> input) {
        ValidateFn validateFn = new ValidateFn(input.getTypeDescriptor());
        PCollectionTuple result =
                input.apply(
                        "ValidateFn",
                        ParDo.of(validateFn)
                                .withOutputTags(validateFn.outputTag, TupleTagList.of(validateFn.invalidTag)));

        return WithInvalids.Result.of(result, validateFn.outputTag, validateFn.invalidTag);
    }

    private class ValidateFn extends DoFn<T, T> {
        final TupleTag<T> outputTag = new TupleTag<T>() {
        };
        final TupleTag<WithInvalids.InvalidElement<T>> invalidTag = new TupleTag<WithInvalids.InvalidElement<T>>() {
        };

        private final TypeDescriptor<T> inputType;

        public ValidateFn(TypeDescriptor<T> inputType){
            this.inputType = inputType;
        }

        @ProcessElement
        public void processElement(@Element T element, MultiOutputReceiver r, ProcessContext c) throws Exception {
            //TODO: Invalid Records Metric
            try {
                if (predicate.apply(element)) {
                    c.output(outputTag, element);
                } else {
                    //Handle Invalid Data
                    WithInvalids.InvalidElement<T> invalidElement = WithInvalids.InvalidElement.of(element);
                    c.output(invalidTag, invalidElement);
                }
            } catch (Exception e) {
                //TODO:Log Exception
                if(exceptionsAsInvalid){
                    WithInvalids.InvalidElement<T> invalidElement = WithInvalids.InvalidElement.of(element,
                            new WithInvalids.InvalidElementException("Validation failed for " + getName(), e));
                    c.output(invalidTag, invalidElement);
                }
                else
                    throw e;
            }
        }

    }
}
