package leap.data.beam.transforms;

import org.apache.beam.sdk.coders.BigIntegerCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;


public class ValidateTest {
    @Rule
    public TestPipeline p = TestPipeline.create();

    @Test
    public void testValidateFnIgnore(){
        List<String> words = Arrays.asList(" some  input  words ", " ", " cool ", " foo", " bar", "a");
        /**
         * Validate words by checking if the length of it is greater than 2
         * Empty element and elements with less than 2 should be moved to invalid collection and ignored
         */
        PCollection<String> validWords =  p.apply(Create.of(words).withCoder(StringUtf8Coder.of()))
                .apply(Validate.by((word) -> word.length() > 2))
                .invalidsIgnored();
        /**
         * " " and "a" should not be in the output
         */
        PAssert.that(validWords).containsInAnyOrder(" some  input  words ", " cool ", " foo", " bar");
        p.run().waitUntilFinish();

    }

    @Test
    public void testValidateFnCollect(){
        List<String> words = Arrays.asList(" some  input  words ", " ", " cool ", " foo", " bar","a");
        List<PCollection<WithInvalids.InvalidElement<String>>> invalids = new ArrayList<>();
        /**
         * Validate words by checking if the length of it is greater than 2
         * Empty element and elements with less than 2 should be moved to invalid collection
         */
        PCollection<String> validWords =  p.apply(Create.of(words).withCoder(StringUtf8Coder.of()))
                .apply(Validate.by((word) -> word.length() > 2))
                .invalidsTo(invalids);
        PAssert.that(validWords).containsInAnyOrder(" some  input  words ", " cool ", " foo", " bar");

        /**
         * " " and "a" should be in the invalid collection
         */
        List<String> invalidWords = Arrays.asList(" ","a");
        PAssert.that(invalids.get(0)).satisfies((invalidElements) -> {
            invalidElements.forEach((invalidElement)->{
                assertThat("Element should be in list",invalidWords.contains(invalidElement.element()));
            });
            return null;
        });
        p.run().waitUntilFinish();
    }

    @Test
    public void testValidateFnExceptionCollect(){
        List<Integer> numbers = Arrays.asList(0,1,2,3);
        List<Integer> invalidNumbers = Arrays.asList(0,3);
        List<PCollection<WithInvalids.InvalidElement<Integer>>> invalids = new ArrayList<>();
        PCollection<Integer> validNumbers =  p.apply(Create.of(numbers).withCoder(VarIntCoder.of()))
                .apply(Validate.by((Integer number) -> (8 / number) > 3)
                        .withExceptionsAsInvalid()
                        )
                .invalidsTo(invalids);
        PAssert.that(validNumbers).containsInAnyOrder(1,2);
        PAssert.that(invalids.get(0)).satisfies((invalidElements) -> {
            invalidElements.forEach((invalidElement)->{
                assertThat("Element should be in list",invalidNumbers.contains(invalidElement.element()));
                if(invalidElement.element() == 0){
                    assertThat("Should throw exception",invalidElement.exception().getCause() instanceof ArithmeticException);
                }
            });
            return null;
        });
        p.run().waitUntilFinish();
    }
}
