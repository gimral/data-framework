package leap.data.beam.logging;

import org.apache.beam.sdk.transforms.DoFn;


/**
 * LifeCylce of a DoFn is
 * Setup
 * Repeatedly Call
 *  StartBundle
 *  Repeat ProcessElement
 *  Finish Bundle
 * TearDown
 * @param <InputT>
 * @param <OutPutT>
 */
public class DoFnLoggingExtension<InputT,OutPutT> extends DoFn<InputT,OutPutT> {
    @Setup
    public void setup() {

    }
}
