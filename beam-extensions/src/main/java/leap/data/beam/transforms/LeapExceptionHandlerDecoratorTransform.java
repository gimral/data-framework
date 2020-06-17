package leap.data.beam.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * Decorator transform to handle exceptions encountered while processing any DoFn
 *
 * <p>This decorator is responsible for processing the wrapped DoFn and capturing any exceptions.
 * Captured exceptions will be added to an invalid collection
 *
 * <p>See {@link WithInvalids} documentation for usage patterns of the returned {@link
 * WithInvalids.Result}.
 *
 */
public class LeapExceptionHandlerDecoratorTransform<InputT,OutputT> extends PTransform<PCollection<InputT>,
        WithInvalids.Result<PCollection<OutputT>, WithInvalids.InvalidElement<InputT>>> {
    private final LeapDoFnBase<InputT,OutputT> decoratedDoFn;

    public LeapExceptionHandlerDecoratorTransform(LeapDoFnBase<InputT,OutputT> decoratedDoFn) {
        this.decoratedDoFn = decoratedDoFn;
    }

    @Override
    public WithInvalids.Result<PCollection<OutputT>, WithInvalids.InvalidElement<InputT>> expand(PCollection<InputT> input) {
        LeapExceptionHandlerDoFn exceptionHandlerDoFn = new LeapExceptionHandlerDoFn();

        PCollectionTuple result =
                input.apply(
                        LeapExceptionHandlerDecoratorTransform.class.getSimpleName(),
                        ParDo.of(exceptionHandlerDoFn)
                                .withOutputTags(decoratedDoFn.getOutputTag(), TupleTagList.of(exceptionHandlerDoFn.invalidTag)));
        return WithInvalids.Result.of(result, decoratedDoFn.getOutputTag(), exceptionHandlerDoFn.invalidTag);
    }


    private class LeapExceptionHandlerDoFn extends LeapDoFnBase<InputT,OutputT> {
        final TupleTag<WithInvalids.InvalidElement<InputT>> invalidTag = new TupleTag<WithInvalids.InvalidElement<InputT>>() {
        };

        @ProcessElement
        public void processElement(@Element InputT element, ProcessContext c) {
            try{

                decoratedDoFn.processElement(element, c);
            }
            catch (Exception e){
                WithInvalids.InvalidElement<InputT> invalidElement = WithInvalids.InvalidElement.of(element,
                        new WithInvalids.InvalidElementException(e));
                c.output(invalidTag, invalidElement);
            }
        }

    }


}


