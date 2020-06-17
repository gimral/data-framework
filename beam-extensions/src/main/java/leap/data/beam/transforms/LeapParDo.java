package leap.data.beam.transforms;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.transforms.ProcessFunction;

/**
 * Base ParDo to handle cross cutting concerns
 *
 * <p>Example usage: To handle any exception
 *
 * <pre>{@code
 * Result<PCollection<String>, String>> result = words.apply(
 *     LeapParDo.of(...) //Any DoFn //If an exception occurs move input element into invalid collection
 *              );
 * PCollection<Integer> output = result.output();
 * PCollection<String> invalids = result.invalids();
 * }</pre>
 *
 */
@AutoValue
public abstract class LeapParDo<InputT, OutputT>{

//    private LeapDoFn<InputT, OutputT> fn;
//
//    abstract boolean getExceptionHandlerDecorated();
//
//    abstract Builder toBuilder();
//
//    @AutoValue.Builder
//    abstract static class Builder {
//        abstract Builder setExceptionHandlerDecorated(boolean exceptionHandlerDecorated);
//        abstract LeapParDo build();
//    }
//
//    public LeapParDo withExceptionsAsInvalid(){
//        return toBuilder().setExceptionHandlerDecorated(true).build();
//    }

    /**
     * Returns a new LeapExceptionHandlerDecoratorTransform by decorating the supplied DoFn by it
     */
    public static <InputT, OutputT> LeapExceptionHandlerDecoratorTransform<InputT, OutputT>
    of(LeapDoFn<InputT, OutputT> fn) {
        return new LeapExceptionHandlerDecoratorTransform<>(fn);
    }
}
