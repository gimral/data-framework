//package leap.data.beam.transforms;
//
//import org.apache.beam.sdk.transforms.DoFn;
//import org.apache.beam.sdk.transforms.PTransform;
//import org.apache.beam.sdk.transforms.ParDo;
//import org.apache.beam.sdk.values.PCollection;
//
//public class ExceptionHandler {
//
//    public static class ExceptionHandlerTransform<InputT,OutputT> extends PTransform<PCollection<InputT>,
//        WithInvalids.Result<PCollection<OutputT>, WithInvalids.InvalidElement<InputT>>>{
//        private DoFn<InputT,OutputT> decoratedDoFn;
//        @Override
//        public WithInvalids.Result<PCollection<OutputT>, WithInvalids.InvalidElement<InputT>> expand(PCollection<InputT> input) {
//            ParDo
//            input.apply(ParDo.of())
//            return null;
//        }
//    }
//}
