package leap.data.beam.transforms.join;

import org.apache.beam.sdk.coders.BooleanCoder;
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
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class OneToManyJoin<K, L, R> extends PTransform<PCollection<KV<K, L>>,
        WithDroppedJoinElements.Result<K, L, R>> {

    public static <K, L, R> OneToManyJoin<K, L, R> inner(PCollection<KV<K, R>> rightCollection) {
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

        if(!leftCollection.getWindowingStrategy().isTriggerSpecified()){
            leftCollection = leftCollection.apply("Left Collection Global Window", Window.<KV<K, L>>into(new GlobalWindows())
                    .withTimestampCombiner(TimestampCombiner.EARLIEST)
                    .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                    .discardingFiredPanes()
                    .withAllowedLateness(Duration.ZERO));
        }
        if(!rightCollection.getWindowingStrategy().isTriggerSpecified()){
            rightCollection = rightCollection.apply("Right Collection Global Window", Window.<KV<K, R>>into(new GlobalWindows())
                    .withTimestampCombiner(TimestampCombiner.EARLIEST)
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

        private static final String LEFT_STATE = "leftState";
        private static final String JOINED_STATE = "joinedState";
        private static final String RIGHT_COLLECTION_STATE = "rightCollectionState";
        private static final String LEFT_STATE_EXPIRING = "leftCollectionStateExpiring";
        private static final String RIGHT_COLLECTION_STATE_EXPIRING = "rightCollectionStateExpiring";

        @TimerId(LEFT_STATE_EXPIRING)
        private final TimerSpec leftStateExpiryTimerSpec;

        @TimerId(RIGHT_COLLECTION_STATE_EXPIRING)
        private final TimerSpec rightCollectionStateExpiryTimerSpec;

        @StateId(LEFT_STATE)
        private final StateSpec<ValueState<L>> leftState;

        @StateId(JOINED_STATE)
        private final StateSpec<ValueState<Boolean>> joinedState;

        @StateId(RIGHT_COLLECTION_STATE)
        private final StateSpec<BagState<R>> rightCollectionState;

        private final Duration leftStateExpireDuration;
        private final Duration rightStateExpireDuration;

        private final Counter droppedRightElements;
        private final Counter droppedLeftElements;

        public OneToManyJoinDoFn(TimeDomain timeDomain,
                                 Duration leftStateExpireDuration, Duration rightStateExpireDuration,
                                 Coder<L> leftCollectionCoder, Coder<R> rightCollectionCoder
        ) {
            leftStateExpiryTimerSpec = TimerSpecs.timer(timeDomain);
            rightCollectionStateExpiryTimerSpec = TimerSpecs.timer(timeDomain);
            leftState = StateSpecs.value(leftCollectionCoder);
            joinedState = StateSpecs.value(BooleanCoder.of());
            rightCollectionState = StateSpecs.bag(rightCollectionCoder);
            this.leftStateExpireDuration = leftStateExpireDuration;
            this.rightStateExpireDuration = rightStateExpireDuration;

            droppedLeftElements = Metrics.counter("name", "droppedLeftElements");
            droppedRightElements = Metrics.counter("name", "droppedRightElements");
        }

        @ProcessElement
        public void processElement(ProcessContext c,
                                   @TimerId(LEFT_STATE_EXPIRING) Timer leftStateExpiryTimer,
                                   @TimerId(RIGHT_COLLECTION_STATE_EXPIRING) Timer rightCollectionStateExpiryTimer,
                                   @StateId(LEFT_STATE) ValueState<L> leftState,
                                   @StateId(JOINED_STATE) ValueState<Boolean> joinedState,
                                   @StateId(RIGHT_COLLECTION_STATE) BagState<R> rightCollectionState) {
            Boolean joined = false;
            Boolean newLeftValue = false;
            L leftValue = null;
            Iterable<L> leftElements = c.element().getValue().getAll(leftTupleTag);
            for (L leftElemet :
                    leftElements) {
                leftValue = leftElemet;
                newLeftValue = true;
                break;
            }
            if (leftValue == null) {
                leftValue = leftState.read();
            }
            //Left value exists emit all right values
            //TODO: Maybe do not read left collection state until we are sure there are elements in right
            if (leftValue != null) {
                Iterable<R> rightElements = c.element().getValue().getAll(rightTupleTag);
                for (R rightElement : rightElements) {
                    KV<L, R> joinedElement = KV.of(leftValue, rightElement);
                    KV<K, KV<L, R>> keyedJoinedElement = KV.of(c.element().getKey(), joinedElement);
                    joined = true;
                    c.output(keyedJoinedElement);
                }
                //New left value is encountered emit all right elements from state
                if (newLeftValue) {
                    Iterable<R> pendingRightElements = rightCollectionState.read();
                    for (R rightElement :
                            pendingRightElements) {
                        KV<L, R> joinedElement = KV.of(leftValue, rightElement);
                        KV<K, KV<L, R>> keyedJoinedElement = KV.of(c.element().getKey(), joinedElement);
                        joined = true;
                        c.output(keyedJoinedElement);
                    }
                    rightCollectionState.clear();
                    leftState.write(leftValue);
                    leftStateExpiryTimer.offset(leftStateExpireDuration).setRelative();
                }
                if(joined)
                    joinedState.write(true);
                return;
            }

            Iterable<R> rightElements = c.element().getValue().getAll(rightTupleTag);
            boolean rightElementsExists = false;
            for (R rightElement : rightElements) {
                rightCollectionState.add(rightElement);
                rightElementsExists= true;
            }
            if(rightElementsExists)
                rightCollectionStateExpiryTimer.offset(rightStateExpireDuration).setRelative();
        }

        @OnTimer(LEFT_STATE_EXPIRING)
        public void onLeftCollectionStateExpire(OnTimerContext c,
                                                @StateId(LEFT_STATE) ValueState<L> leftState,
                                                @StateId(JOINED_STATE) ValueState<Boolean> joinedState) {
            L leftValue = leftState.read();
            //Not possible for it to be null but just to be sure
            if(leftValue != null) {
                Boolean joined = joinedState.read();
                if(joined == null || !joined){
                    droppedLeftElements.inc();
                    c.output(leftTupleTag, leftValue);
                }
                logger.debug("Clearing Left State for {}", leftValue);
            }
            leftState.clear();
            joinedState.clear();
        }

        @OnTimer(RIGHT_COLLECTION_STATE_EXPIRING)
        public void onRightCollectionStateExpire(OnTimerContext c,
                                                 @StateId(RIGHT_COLLECTION_STATE) BagState<R> rightCollectionState) {
            Iterable<R> droppedElements = rightCollectionState.read();
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
