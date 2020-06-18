package leap.data.beam.transforms;

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

public class LeapExceptionHandlerDoFnTest {
    @Rule
    public TestPipeline p = TestPipeline.create();

    @Test
    public void testExceptionsHandled(){
        List<Integer> numbers = Arrays.asList(0,1,2,3);
        /**
         * Run a simple DoFn that will generate exception for element 0 and ignore that element returning remaining elements
         */
        PCollection<String> validNumbers = p.apply(Create.of(numbers).withCoder(VarIntCoder.of()))
                .apply(LeapParDo.ofExcetionHandler(getDoFn()))
                .invalidsIgnored() //ignore the elements that generates exceptions
                .setCoder(StringUtf8Coder.of());
        PAssert.that(validNumbers).containsInAnyOrder("8","4","2");
        p.run().waitUntilFinish();
    }

    private static LeapDoFn<Integer,String> getDoFn(){
        /* Process will fail for element 0 with Divide by zero exception */
        return new LeapDoFn<Integer, String>(){
            @Override
            protected void innerProcessElement(Integer element, ProcessContext c) {
                Integer output = 8 / element;
                c.output(output.toString());
            }
        };
    }
}
