package leap.data.beam.transforms.join;

import org.apache.beam.sdk.coders.*;
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

import java.math.BigInteger;
import java.util.Optional;

public class OneToOneJoin<K, L, R> extends PTransform<PCollection<KV<K, L>>,
        WithDroppedJoinElements.Result<K, L, R>> {

    public static <K, L, R> OneToOneJoin<K, L, R> inner(PCollection<KV<K, R>> rightCollection) {
        return new OneToOneJoin<>(rightCollection);
    }

    private static final Logger logger = LoggerFactory.getLogger(OneToOneJoin.class);
    private transient PCollection<KV<K, R>> rightCollection;
    private final Duration leftStateExpireDuration;
    private final Duration rightStateExpireDuration;

    final TupleTag<KV<K, KV<L, R>>> outputTag = new TupleTag<KV<K, KV<L, R>>>(){};
    final TupleTag<L> leftTupleTag = new TupleTag<L>(){};
    final TupleTag<R> rightTupleTag = new TupleTag<R>(){};

    public OneToOneJoin(PCollection<KV<K, R>> rightCollection) {
        this.rightCollection = rightCollection;
        this.leftStateExpireDuration = Duration.standardSeconds(30);
        this.rightStateExpireDuration = Duration.standardSeconds(30);
    }

    public OneToOneJoin(PCollection<KV<K, R>> rightCollection, Duration leftStateExpireDuration, Duration rightStateExpireDuration) {
        this.rightCollection = rightCollection;
        this.leftStateExpireDuration = leftStateExpireDuration;
        this.rightStateExpireDuration = rightStateExpireDuration;
    }

    public OneToOneJoin<K, L, R> withLeftStateExpireDuration(Duration leftStateExpireDuration) {
        return new OneToOneJoin<>(this.rightCollection, leftStateExpireDuration, this.rightStateExpireDuration);
    }

    public OneToOneJoin<K, L, R> withRightStateExpireDuration(Duration rightStateExpireDuration) {
        return new OneToOneJoin<>(this.rightCollection, this.leftStateExpireDuration, rightStateExpireDuration);
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
        else if(!leftCollection.getWindowingStrategy().isTriggerSpecified()){
            leftCollection = leftCollection.apply("Left Collection Global Window", Window.<KV<K, L>>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                    .discardingFiredPanes()
                    .withAllowedLateness(Duration.ZERO));
        }
        else if(!rightCollection.getWindowingStrategy().isTriggerSpecified()){
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
                new OneToOneJoinDoFn(TimeDomain.EVENT_TIME,
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



    private class OneToOneJoinDoFn extends DoFn<KV<K, CoGbkResult>,
            KV<K, KV<L, R>>> {

        private static final String LEFT_STATE = "leftState";
        private static final String JOINED_STATE = "joinedState";
        private static final String RIGHT_STATE = "rightState";
        private static final String LEFT_STATE_EXPIRING = "leftCollectionStateExpiring";
        private static final String RIGHT_STATE_EXPIRING = "rightStateExpiring";

        @TimerId(LEFT_STATE_EXPIRING)
        private final TimerSpec leftStateExpiryTimerSpec;

        @TimerId(RIGHT_STATE_EXPIRING)
        private final TimerSpec rightStateExpiryTimerSpec;

        @StateId(LEFT_STATE)
        private final StateSpec<ValueState<L>> leftState;

        @StateId(JOINED_STATE)
        private final StateSpec<ValueState<Boolean>> joinedState;

        @StateId(RIGHT_STATE)
        private final StateSpec<ValueState<R>> rightState;

        private final Duration leftStateExpireDuration;
        private final Duration rightStateExpireDuration;

        private final Counter droppedRightElements;
        private final Counter droppedLeftElements;

        public OneToOneJoinDoFn(TimeDomain timeDomain,
                                 Duration leftStateExpireDuration, Duration rightStateExpireDuration,
                                 Coder<L> leftCollectionCoder, Coder<R> rightCollectionCoder
        ) {
            leftStateExpiryTimerSpec = TimerSpecs.timer(timeDomain);
            rightStateExpiryTimerSpec = TimerSpecs.timer(timeDomain);
            leftState = StateSpecs.value(leftCollectionCoder);
            joinedState = StateSpecs.value(BooleanCoder.of());
            rightState = StateSpecs.value(rightCollectionCoder);
            this.leftStateExpireDuration = leftStateExpireDuration;
            this.rightStateExpireDuration = rightStateExpireDuration;

            droppedLeftElements = Metrics.counter("name", "droppedLeftElements");
            droppedRightElements = Metrics.counter("name", "droppedRightElements");
        }

        @ProcessElement
        public void processElement(ProcessContext c,
                                   @TimerId(LEFT_STATE_EXPIRING) Timer leftStateExpiryTimer,
                                   @TimerId(RIGHT_STATE_EXPIRING) Timer rightStateExpiryTimer,
                                   @StateId(LEFT_STATE) ValueState<L> leftState,
                                   @StateId(JOINED_STATE) ValueState<Boolean> joinedState,
                                   @StateId(RIGHT_STATE) ValueState<R> rightState) {
            Optional<L> leftValue = Optional.empty();
            Optional<R> rightValue = Optional.empty();
            Iterable<L> leftElements = c.element().getValue().getAll(leftTupleTag);
            for (L leftElemet :
                    leftElements) {
                leftValue = Optional.of(leftElemet);
                //newLeftValue = true;
                break;
            }
            Iterable<R> rightElements = c.element().getValue().getAll(rightTupleTag);
            for (R rightElement :
                    rightElements) {
                rightValue = Optional.of(rightElement);
                //newLeftValue = true;
                break;
            }
            if(!leftValue.isPresent() && !rightValue.isPresent())
                return;
            Boolean joined = joinedState.read();
            if(joined != null && joined)
                return;
            if(!rightValue.isPresent()) {
                rightValue = Optional.ofNullable(rightState.read());
                if(!rightValue.isPresent()) {
                    leftState.write(leftValue.get());
                    leftStateExpiryTimer.offset(leftStateExpireDuration).setRelative();
                    return;
                }
            }
            if(!leftValue.isPresent()) {
                leftValue = Optional.ofNullable(leftState.read());
                if(!leftValue.isPresent()){
                    rightState.write(rightValue.get());
                    rightStateExpiryTimer.offset(rightStateExpireDuration).setRelative();
                    return;
                }
            }
            KV<L, R> joinedElement = KV.of(leftValue.get(), rightValue.get());
            KV<K, KV<L, R>> keyedJoinedElement = KV.of(c.element().getKey(), joinedElement);
            c.output(keyedJoinedElement);
            leftState.clear();
            rightState.clear();
            joinedState.write(true);

        }

        @OnTimer(LEFT_STATE_EXPIRING)
        public void onLeftCollectionStateExpire(OnTimerContext c,
                                                @StateId(LEFT_STATE) ValueState<L> leftState,
                                                @StateId(JOINED_STATE) ValueState<Boolean> joinedState) {
            L leftValue = leftState.read();
            if(leftValue != null) {
                droppedLeftElements.inc();
                c.output(leftTupleTag, leftValue);
                logger.debug("Clearing Left State for {}", leftValue);
            }
            leftState.clear();
            joinedState.clear();
        }

        @OnTimer(RIGHT_STATE_EXPIRING)
        public void onRightStateExpire(OnTimerContext c,
                                       @StateId(RIGHT_STATE) ValueState<R> rightState,
                                       @StateId(JOINED_STATE) ValueState<Boolean> joinedState) {
            R rightValue = rightState.read();
            if(rightValue != null) {
                droppedRightElements.inc();
                c.output(rightTupleTag, rightValue);
                logger.debug("Clearing Right State for {}", rightValue);
            }
            rightState.clear();
            joinedState.clear();
        }

    }
}
