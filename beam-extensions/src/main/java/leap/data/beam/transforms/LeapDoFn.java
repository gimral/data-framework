package leap.data.beam.transforms;

import org.apache.beam.sdk.transforms.DoFn;

public abstract class LeapDoFn<InputT,OutputT> extends LeapDoFnBase<InputT,OutputT> {

//    public DoFn<InputT,OutputT> viaExceptionsHandled(){
//        return new LeapExceptionHandlerDoFn<>(this);
//    }

    @Override
    @ProcessElement
    public void processElement(@Element InputT element, DoFn.MultiOutputReceiver r) {
        innerProcessElement(element, r.get(getOutputTag()));
    }

    abstract protected void innerProcessElement(InputT element, OutputReceiver r);
}
