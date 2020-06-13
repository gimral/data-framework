package leap.data.beam.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

public abstract class LeapDoFnBase<InputT,OutputT> extends DoFn<InputT,OutputT>{
    private final TupleTag<OutputT> outputTag = new TupleTag<OutputT>() {
    };
    public abstract void processElement(InputT element, ProcessContext c);

    public TupleTag<OutputT> getOutputTag() {
        return outputTag;
    }
}
