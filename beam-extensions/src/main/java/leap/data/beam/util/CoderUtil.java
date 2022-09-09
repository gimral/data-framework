package leap.data.beam.util;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class CoderUtil {

    public <K,V> Coder<V> getValueCoder(PCollection<KV<K, V>> pCollection, boolean isNullable) {
        Coder<?> kvCoder = pCollection.getCoder();
        if(kvCoder instanceof NullableCoder<?>){
            kvCoder = ((NullableCoder<?>)kvCoder).getValueCoder();
        }
        if (!(kvCoder instanceof KvCoder<?, ?>))
            throw new IllegalArgumentException("PCollection does not use a KVCoder");
        @SuppressWarnings("unchecked")
        KvCoder<K, V> coder = (KvCoder<K, V>) kvCoder;
        if(isNullable)
            return NullableCoder.of(coder.getValueCoder());
        return coder.getValueCoder();
    }

    public <K,V> Coder<K> getKeyCoder(PCollection<KV<K, V>> pCollection) {
        Coder<?> kvCoder = pCollection.getCoder();
        if (!(kvCoder instanceof KvCoder<?, ?>))
            throw new IllegalArgumentException("PCollection does not use a KVCoder");
        @SuppressWarnings("unchecked")
        KvCoder<K, V> coder = (KvCoder<K, V>) kvCoder;
        return coder.getKeyCoder();
    }
}
