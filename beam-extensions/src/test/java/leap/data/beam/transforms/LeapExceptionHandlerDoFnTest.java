package leap.data.beam.transforms;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
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
        PCollection<String> validNumbers = p.apply(Create.of(numbers).withCoder(VarIntCoder.of()))
                .apply(LeapParDo.of(getDoFn()))
                .invalidsIgnored()
                .setCoder(StringUtf8Coder.of());
        PAssert.that(validNumbers).containsInAnyOrder("8","4","2");
        p.run().waitUntilFinish();
    }

    private static LeapDoFn<Integer,String> getDoFn(){
        return new LeapDoFn<>(){
            @Override
            protected void innerProcessElement(Integer element, OutputReceiver r) {
                Integer output = 8 / element;
                r.output(output.toString());
            }
        };
    }
}
