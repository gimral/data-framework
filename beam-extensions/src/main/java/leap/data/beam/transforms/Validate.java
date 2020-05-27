package leap.data.beam.transforms;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.*;

public class Validate {

    public static <T, PredicateT extends ProcessFunction<T, Boolean>> ValidateTransform<T> via(
            PredicateT predicate) {
        return new AutoValue_Validate_ValidateTransform.Builder<T>()
                .setPredicate(predicate)
                .setPredicateDescription("Validate")
                .setExceptionsAsInvalid(false)
                .build();
    }

    public static <T, PredicateT extends ProcessFunction<T, Boolean>> ValidateTransform<T> via(
            String predicateDescription, PredicateT predicate) {
        return new AutoValue_Validate_ValidateTransform.Builder<T>()
                .setPredicate(predicate)
                .setPredicateDescription(predicateDescription)
                .setExceptionsAsInvalid(false)
                .build();
    }

    /** Binary compatibility adapter for {@link #via(ProcessFunction)}. */
    public static <T> ValidateTransform<T> via(
            SerializableFunction<T, Boolean> predicate) {
        return via((ProcessFunction<T, Boolean>) predicate);
    }

    @AutoValue
    public abstract static class ValidateTransform<T> extends PTransform<PCollection<T>,
            WithInvalids.Result<PCollection<T>, WithInvalids.InvalidElement<T>>> {
        //todo:side inputs

        abstract ProcessFunction<T, Boolean> getPredicate();
        abstract String getPredicateDescription();
        abstract boolean getExceptionsAsInvalid();

        abstract Builder<T> toBuilder();

        @AutoValue.Builder
        abstract static class Builder<T> {
            abstract Builder<T> setPredicate(ProcessFunction<T, Boolean> predicate);
            abstract Builder<T> setPredicateDescription(String predicateDescription);
            abstract Builder<T> setExceptionsAsInvalid(boolean exceptionsAsInvalid);
            abstract ValidateTransform<T> build();
        }

        public <T> ValidateTransform<T> withExceptionsAsInvalid(){
            return (ValidateTransform<T>) toBuilder().setExceptionsAsInvalid(true).build();
        }

        @Override
        public WithInvalids.Result<PCollection<T>, WithInvalids.InvalidElement<T>> expand(PCollection<T> input) {
            ValidateFn validateFn = new ValidateFn(input.getTypeDescriptor());


            PCollectionTuple result =
                    input.apply(
                    Validate.class.getSimpleName(),
                    ParDo.of(validateFn)
                            .withOutputTags(validateFn.outputTag, TupleTagList.of(validateFn.invalidTag)));
            return WithInvalids.Result.of(result, validateFn.outputTag, validateFn.invalidTag);
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            builder.add(DisplayData.item("predicate", getPredicateDescription()).withLabel("Validate Predicate"));
        }


        private class ValidateFn extends DoFn<T, T> {
            final TupleTag<T> outputTag = new TupleTag<>() {
            };
            final TupleTag<WithInvalids.InvalidElement<T>> invalidTag = new TupleTag<>() {
            };

            private final TypeDescriptor<T> inputType;

            public ValidateFn(TypeDescriptor<T> inputType){
                this.inputType = inputType;
            }

            @ProcessElement
            public void processElement(@Element T element, MultiOutputReceiver r, ProcessContext c) throws Exception {
                //TODO: Invalid Records Metric
                try {
                    if (getPredicate().apply(element)) {
                        r.get(outputTag).output(element);
                    } else {
                        //Handle Invalid Data
                        WithInvalids.InvalidElement<T> invalidElement = WithInvalids.InvalidElement.of(element);
                        r.get(invalidTag).output(invalidElement);
                    }
                } catch (Exception e) {
                    //TODO:Log Exception
                    if(getExceptionsAsInvalid()){
                        WithInvalids.InvalidElement<T> invalidElement = WithInvalids.InvalidElement.of(element, e);
                        r.get(invalidTag).output(invalidElement);
                    }
                    else
                        throw e;
                }
            }

//            @Override
//            public TypeDescriptor<T> getInputTypeDescriptor() {
//                return inputType;
//            }
//
//            @Override
//            public TypeDescriptor<T> getOutputTypeDescriptor() {
//                return inputType;
//            }

        }
    }
}
