package leap.data.beam.logging;

import leap.data.beam.core.LeapDoFn;
import leap.data.framework.core.logging.EventDataLogger;
import org.apache.beam.sdk.values.TypeDescriptor;

public class LeapEventDataLogDecoratorDoFn<InputT,OutputT> extends LeapDoFn<InputT,OutputT> {
    private final LeapDoFn<InputT,OutputT> decoratedDoFn;
    private BeamEventDataLogger eventDataLogger;

    public LeapEventDataLogDecoratorDoFn(LeapDoFn<InputT, OutputT> decoratedDoFn) {
        this.decoratedDoFn = decoratedDoFn;
    }

    @StartBundle
    public void startBundle(){
        //Had to initialize because of the serialization need
        eventDataLogger = new BeamEventDataLogger();
    }

    @Override
    protected void innerProcessElement(InputT element, ProcessContext c) {
        decoratedDoFn.processElement(element, c);
        //TODO: It could be possible to get the step name from the thread name
        eventDataLogger.log(element);
    }

    @Override
    public TypeDescriptor<OutputT> getOutputTypeDescriptor() {
        return decoratedDoFn.getOutputTypeDescriptor();
    }
}
