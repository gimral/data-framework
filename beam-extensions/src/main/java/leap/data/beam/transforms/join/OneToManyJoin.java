package leap.data.beam.transforms.join;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class OneToManyJoin<K, L, R> extends PTransform<PCollection<KV<K, L>>,
        WithDroppedJoinElements.Result<K, L, R>> {

    public static <K, L, R> OneToManyJoin<K, L, R> connect(PCollection<KV<K, R>> rightCollection) {
        return new OneToManyJoin<>(rightCollection);
    }

    private static final Logger logger = LoggerFactory.getLogger(OneToManyJoin.class);
    private transient PCollection<KV<K, R>> rightCollection;
    private final Duration leftStateExpireDuration;
    private final Duration rightStateExpireDuration;

    final TupleTag<KV<K, KV<L, R>>> outputTag = new TupleTag<KV<K, KV<L, R>>>(){};
    final TupleTag<L> leftTupleTag = new TupleTag<L>(){};
    final TupleTag<R> rightTupleTag = new TupleTag<R>(){};

    public OneToManyJoin(PCollection<KV<K, R>> rightCollection) {
        this.rightCollection = rightCollection;
        this.leftStateExpireDuration = Duration.standardSeconds(30);
        this.rightStateExpireDuration = Duration.standardSeconds(30);
    }

    public OneToManyJoin(PCollection<KV<K, R>> rightCollection, Duration leftStateExpireDuration, Duration rightStateExpireDuration) {
        this.rightCollection = rightCollection;
        this.leftStateExpireDuration = leftStateExpireDuration;
        this.rightStateExpireDuration = rightStateExpireDuration;
    }

    public OneToManyJoin<K, L, R> withLeftStateExpireDuration(Duration leftStateExpireDuration) {
        return new OneToManyJoin<>(this.rightCollection, leftStateExpireDuration, this.rightStateExpireDuration);
    }

    public OneToManyJoin<K, L, R> withRightStateExpireDuration(Duration rightStateExpireDuration) {
        return new OneToManyJoin<>(this.rightCollection, this.leftStateExpireDuration, rightStateExpireDuration);
    }

    @Override
    public WithDroppedJoinElements.Result<K, L, R> expand(PCollection<KV<K, L>> leftCollection) {

        if (!leftCollection.getWindowingStrategy().isTriggerSpecified()
                && !rightCollection.getWindowingStrategy().isTriggerSpecified()) {
            leftCollection = leftCollection.apply("Left Collection Global Window", Window.<KV<K, L>>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                    .discardingFiredPanes()
                    .withAllowedLateness(Duration.ZERO));
            rightCollection = rightCollection.apply("Right Collection Global Window", Window.<KV<K, R>>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                    .discardingFiredPanes()
                    .withAllowedLateness(Duration.ZERO));
        }

        PCollection<KV<K, CoGbkResult>> coGroupByResult =
                KeyedPCollectionTuple.of(leftTupleTag, leftCollection)
                        .and(rightTupleTag, rightCollection)
                        .apply("CoGroupBy", CoGroupByKey.create());

        PCollectionTuple joinedResult = coGroupByResult.apply("Join", ParDo.of(
                new OneToManyJoinDoFn(TimeDomain.EVENT_TIME,
                        leftStateExpireDuration, rightStateExpireDuration,
                        getValueCoder(leftCollection), getValueCoder(rightCollection)))
                .withOutputTags(outputTag, TupleTagList.of(leftTupleTag).and(rightTupleTag)));

        return WithDroppedJoinElements.Result.of(joinedResult,outputTag,
                leftTupleTag,rightTupleTag,KvCoder.of(getKeyCoder(leftCollection),
                        KvCoder.of(getValueCoder(leftCollection), getValueCoder(rightCollection))));
    }

    private <V> Coder<V> getValueCoder(PCollection<KV<K, V>> pCollection) {
        Coder<?> kvCoder = pCollection.getCoder();
        if (!(kvCoder instanceof KvCoder<?, ?>))
            throw new IllegalArgumentException("PCollection does not use a KVCoder");
        @SuppressWarnings("unchecked")
        KvCoder<K, V> coder = (KvCoder<K, V>) kvCoder;
        return coder.getValueCoder();
    }

    private <V> Coder<K> getKeyCoder(PCollection<KV<K, V>> pCollection) {
        Coder<?> kvCoder = pCollection.getCoder();
        if (!(kvCoder instanceof KvCoder<?, ?>))
            throw new IllegalArgumentException("PCollection does not use a KVCoder");
        @SuppressWarnings("unchecked")
        KvCoder<K, V> coder = (KvCoder<K, V>) kvCoder;
        return coder.getKeyCoder();
    }



    private class OneToManyJoinDoFn extends DoFn<KV<K, CoGbkResult>,
            KV<K, KV<L, R>>> {

        private static final String LEFT_COLLECTION_STATE = "leftCollectionState";
        private static final String RIGHT_COLLECTION_STATE = "rightCollectionState";
        private static final String LEFT_COLLECTION_STATE_EXPIRING = "leftCollectionStateExpiring";
        private static final String RIGHT_COLLECTION_STATE_EXPIRING = "rightCollectionStateExpiring";

        @TimerId(LEFT_COLLECTION_STATE_EXPIRING)
        private final TimerSpec leftCollectionExpiryTimerSpec;

        @TimerId(RIGHT_COLLECTION_STATE_EXPIRING)
        private final TimerSpec rightCollectionExpiryTimerSpec;

        @StateId(LEFT_COLLECTION_STATE)
        private final StateSpec<ValueState<L>> leftCollectionState;

        @StateId(RIGHT_COLLECTION_STATE)
        private final StateSpec<ValueState<List<R>>> rightCollectionState;

        private final Duration leftStateExpireDuration;
        private final Duration rightStateExpireDuration;

        private final Counter droppedRightElements;

        public OneToManyJoinDoFn(TimeDomain timeDomain,
                                 Duration leftStateExpireDuration, Duration rightStateExpireDuration,
                                 Coder<L> leftCollectionCoder, Coder<R> rightCollectionCoder
        ) {
            leftCollectionExpiryTimerSpec = TimerSpecs.timer(timeDomain);
            rightCollectionExpiryTimerSpec = TimerSpecs.timer(timeDomain);
            leftCollectionState = StateSpecs.value(leftCollectionCoder);
            rightCollectionState = StateSpecs.value(ListCoder.of(rightCollectionCoder));
            this.leftStateExpireDuration = leftStateExpireDuration;
            this.rightStateExpireDuration = rightStateExpireDuration;

            droppedRightElements = Metrics.counter("name", "droppedRightElements");
        }

        @ProcessElement
        public void processElement(ProcessContext c,
                                   @TimerId(LEFT_COLLECTION_STATE_EXPIRING) Timer leftCollectionExpiryTimer,
                                   @TimerId(RIGHT_COLLECTION_STATE_EXPIRING) Timer rightCollectionExpiryTimer,
                                   @StateId(LEFT_COLLECTION_STATE) ValueState<L> leftCollectionState,
                                   @StateId(RIGHT_COLLECTION_STATE) ValueState<List<R>> rightCollectionState) {

            boolean newLeftValue = false;
            L leftValue = null;
            Iterable<L> leftElements = c.element().getValue().getAll(leftTupleTag);
            for (L leftElemet :
                    leftElements) {
                leftValue = leftElemet;
                newLeftValue = true;
                break;
            }
            if (leftValue == null) {
                leftValue = leftCollectionState.read();
            }
            //Left value exists emit all right values
            //TODO: Maybe do not read left collection state until we are sure there are elements in right
            if (leftValue != null) {
                Iterable<R> rightElements = c.element().getValue().getAll(rightTupleTag);
                for (R rightElement : rightElements) {
                    KV<L, R> joinedElement = KV.of(leftValue, rightElement);
                    KV<K, KV<L, R>> keyedJoinedElement = KV.of(c.element().getKey(), joinedElement);
                    c.output(keyedJoinedElement);
                }
                //New left value is encountered emit all right elements from state
                if (newLeftValue) {
                    List<R> pendingRightElements = rightCollectionState.read();
                    if (pendingRightElements != null) {
                        for (R rightElement :
                                pendingRightElements) {
                            KV<L, R> joinedElement = KV.of(leftValue, rightElement);
                            KV<K, KV<L, R>> keyedJoinedElement = KV.of(c.element().getKey(), joinedElement);
                            c.output(keyedJoinedElement);
                        }
                        rightCollectionState.clear();
                    }
                    leftCollectionState.write(leftValue);
                    leftCollectionExpiryTimer.offset(leftStateExpireDuration).setRelative();
                }
                return;
            }

            List<R> pendingRightElements = rightCollectionState.read();
            if (pendingRightElements == null) {
                pendingRightElements = new ArrayList<>();
            }

            Iterable<R> rightElements = c.element().getValue().getAll(rightTupleTag);
            for (R rightElement : rightElements) {
                pendingRightElements.add(rightElement);
            }
            rightCollectionState.write(pendingRightElements);
            rightCollectionExpiryTimer.offset(rightStateExpireDuration).setRelative();
        }

        @OnTimer(LEFT_COLLECTION_STATE_EXPIRING)
        public void onLeftCollectionStateExpire(@StateId(LEFT_COLLECTION_STATE) ValueState<L> leftCollectionState) {
            logger.debug("Clearing Left State for {}", leftCollectionState.read());
            leftCollectionState.clear();
        }

        @OnTimer(RIGHT_COLLECTION_STATE_EXPIRING)
        public void onRightCollectionStateExpire(OnTimerContext c,
                                                 @StateId(RIGHT_COLLECTION_STATE) ValueState<List<R>> rightCollectionState) {
            List<R> droppedElements = rightCollectionState.read();
            if (droppedElements != null) {
                for (R droppedElement :
                        droppedElements) {
                    droppedRightElements.inc();
                    logger.debug("Clearing Right State for {}", droppedElement);
                    c.output(rightTupleTag, droppedElement);
                }
                rightCollectionState.clear();
            }
        }

    }
}
