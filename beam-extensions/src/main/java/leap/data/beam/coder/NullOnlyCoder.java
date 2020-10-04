package leap.data.beam.coder;

import org.apache.beam.sdk.coders.AtomicCoder;

import java.io.InputStream;
import java.io.OutputStream;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

public class NullOnlyCoder<T> extends AtomicCoder<T> {
    @Override
    public void encode(T value, OutputStream outStream) {
        checkArgument(value == null, "Can only encode nulls");
        // Encode as no bytes.
    }

    @Override
    public T decode(InputStream inStream) {
        return null;
    }
}
