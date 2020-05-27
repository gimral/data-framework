package leap.data.beam.transforms;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.transforms.ProcessFunction;

@AutoValue
public abstract class LeapParDo{

    abstract boolean getExceptionHandlerDecorated();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
        abstract Builder setExceptionHandlerDecorated(boolean exceptionHandlerDecorated);
        abstract LeapParDo build();
    }

    public LeapParDo withExceptionsAsInvalid(){
        return toBuilder().setExceptionHandlerDecorated(true).build();
    }

    public static <InputT, OutputT> LeapExceptionHandlerDecoratorTransform<InputT, OutputT>
    of(LeapDoFn<InputT, OutputT> fn) {
        return new LeapExceptionHandlerDecoratorTransform<>(fn);
    }
}
