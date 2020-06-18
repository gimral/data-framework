package leap.data.beam.logging;

import leap.data.beam.core.LeapDoFn;
import leap.data.beam.core.LeapParDo;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class LeapEventDataLogDecoratorDoFnTest {
    @Rule
    public TestPipeline p = TestPipeline.create();

    @Test
    public void testLeapEventDataLogDecoratorDoFn(){
        List<Integer> numbers = Arrays.asList(0,1,2,3);
        /**
         * Run a simple DoFn that will just covert integer to string
         */
        PCollection<String> validNumbers = p.apply(Create.of(numbers).withCoder(VarIntCoder.of()))
                .apply(LeapParDo.of(new IntToStringDoFn()))
                .setCoder(StringUtf8Coder.of());
        PAssert.that(validNumbers).containsInAnyOrder("0","1","2","3");
        p.run().waitUntilFinish();
    }

    private static class IntToStringDoFn extends LeapDoFn<Integer,String>{
        @Override
        protected void innerProcessElement(Integer element, ProcessContext c) {
            c.output(element.toString());
        }
    }
}
