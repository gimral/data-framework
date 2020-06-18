package leap.data.beam.core;

import com.google.auto.value.AutoValue;
import leap.data.beam.logging.LeapEventDataLogDecoratorDoFn;
import leap.data.beam.transforms.LeapExceptionHandlerDecoratorTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;

import java.lang.annotation.Annotation;

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

    public static <InputT, OutputT> ParDo.SingleOutput<InputT, OutputT> of(
            LeapDoFn<InputT, OutputT> fn) {
        LeapDoFn<InputT, OutputT> finalDoFn = fn;
        Annotation logEventDataAnnotation = fn.getClass().getAnnotation(LogEventData.class);
        if(logEventDataAnnotation != null)
            finalDoFn = new LeapEventDataLogDecoratorDoFn<>(finalDoFn);
        return ParDo.of(finalDoFn);
    }

    /**
     * Returns a new LeapExceptionHandlerDecoratorTransform by decorating the supplied DoFn by it
     */
    public static <InputT, OutputT> LeapExceptionHandlerDecoratorTransform<InputT, OutputT>
    ofExcetionHandler(LeapDoFn<InputT, OutputT> fn) {
        return new LeapExceptionHandlerDecoratorTransform<>(fn);
    }
}
