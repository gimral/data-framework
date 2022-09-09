package leap.data.beam.transforms.join;

import leap.data.beam.util.CoderUtil;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.join.UnionCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import java.util.ArrayList;
import java.util.List;

public class FlattenJoinPCollections {
private final CoderUtil coderUtil;

    public FlattenJoinPCollections() {
        coderUtil = new CoderUtil();
    }

    public <K,L,R> PCollection<KV<K, RawUnionValue>> flatten(PCollection<KV<K,L>> leftCollection, PCollection<KV<K,R>> rightCollection){
        List<Coder<?>> codersList = new ArrayList<>();
        codersList.add(coderUtil.getValueCoder(leftCollection,false));
        codersList.add(coderUtil.getValueCoder(rightCollection,false));
        UnionCoder unionCoder = UnionCoder.of(codersList);
        Coder<K> keyCoder = coderUtil.getKeyCoder(leftCollection);
        KvCoder<K, RawUnionValue> unionKvCoder = KvCoder.of(keyCoder, unionCoder);

        PCollectionList<KV<K, RawUnionValue>> unionTables = PCollectionList.empty(leftCollection.getPipeline());
        unionTables = unionTables.and(makeUnionTable(0, leftCollection, unionKvCoder));
        unionTables = unionTables.and(makeUnionTable(1, rightCollection, unionKvCoder));

        return unionTables.apply("Flatten", Flatten.pCollections());
    }

    /**
     * Returns a UnionTable for the given input PCollection, using the given union index and the given
     * unionTableEncoder.
     */
    private <K,V> PCollection<KV<K, RawUnionValue>> makeUnionTable(
            final int index,
            PCollection<KV<K, V>> pCollection,
            KvCoder<K, RawUnionValue> unionTableEncoder) {

        return pCollection
                .apply("MakeUnionTable" + index, ParDo.of(new ConstructUnionTableFn<>(index)))
                .setCoder(unionTableEncoder);
    }

    /**
     * A DoFn to construct a UnionTable (i.e., a {@code PCollection<KV<K, RawUnionValue>>} from a
     * {@code PCollection<KV<K, V>>}.
     */
    private static class ConstructUnionTableFn<K, V> extends DoFn<KV<K, V>, KV<K, RawUnionValue>> {

        private final int index;

        public ConstructUnionTableFn(int index) {
            this.index = index;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            KV<K, ?> e = c.element();
            c.output(KV.of(e.getKey(), new RawUnionValue(index, e.getValue())));
        }
    }
}
