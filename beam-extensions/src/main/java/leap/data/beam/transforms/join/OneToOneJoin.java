package leap.data.beam.transforms.join;

import leap.data.beam.util.CoderUtil;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class OneToOneJoin<K, L, R> extends PTransform<PCollection<KV<K, L>>,
        WithDroppedJoinElements.Result<K, L, R>> {

    private static final Logger logger = LoggerFactory.getLogger(OneToOneJoin.class);
    final TupleTag<KV<K, KV<L, R>>> outputTag = new TupleTag<KV<K, KV<L, R>>>() {
    };
    final TupleTag<L> leftTupleTag = new TupleTag<L>() {
    };
    final TupleTag<R> rightTupleTag = new TupleTag<R>() {
    };
    private final Duration leftStateExpireDuration;
    private final Duration rightStateExpireDuration;
    private final JoinType joinType;
    private transient PCollection<KV<K, R>> rightCollection;


    public OneToOneJoin(PCollection<KV<K, R>> rightCollection) {
        this.rightCollection = rightCollection;
        this.leftStateExpireDuration = Duration.standardSeconds(30);
        this.rightStateExpireDuration = Duration.standardSeconds(30);
        this.joinType = JoinType.Inner;
    }
    public OneToOneJoin(PCollection<KV<K, R>> rightCollection, Duration leftStateExpireDuration, Duration rightStateExpireDuration, JoinType joinType) {
        this.rightCollection = rightCollection;
        this.leftStateExpireDuration = leftStateExpireDuration;
        this.rightStateExpireDuration = rightStateExpireDuration;
        this.joinType = joinType;
    }

    public static <K, L, R> OneToOneJoin<K, L, R> inner(PCollection<KV<K, R>> rightCollection) {
        return new OneToOneJoin<>(rightCollection);
    }

    public static <K, L, R> OneToOneJoin<K, L, R> left(PCollection<KV<K, R>> rightCollection) {
        return new OneToOneJoin<K, L, R>(rightCollection).withJoinType(JoinType.Left);
    }

    public static <K, L, R> OneToOneJoin<K, L, R> right(PCollection<KV<K, R>> rightCollection) {
        return new OneToOneJoin<K, L, R>(rightCollection).withJoinType(JoinType.Right);
    }

    public OneToOneJoin<K, L, R> withLeftStateExpireDuration(Duration leftStateExpireDuration) {
        return new OneToOneJoin<>(this.rightCollection, leftStateExpireDuration, this.rightStateExpireDuration, joinType);
    }

    public OneToOneJoin<K, L, R> withRightStateExpireDuration(Duration rightStateExpireDuration) {
        return new OneToOneJoin<>(this.rightCollection, this.leftStateExpireDuration, rightStateExpireDuration, joinType);
    }

    private OneToOneJoin<K, L, R> withJoinType(JoinType joinType) {
        return new OneToOneJoin<>(this.rightCollection, this.leftStateExpireDuration, this.rightStateExpireDuration, joinType);
    }

    @Override
    public WithDroppedJoinElements.Result<K, L, R> expand(PCollection<KV<K, L>> leftCollection) {

        if (!leftCollection.getWindowingStrategy().isTriggerSpecified()) {
            leftCollection = leftCollection.apply("Left Collection Global Window", Window.<KV<K, L>>into(new GlobalWindows())
                    .withTimestampCombiner(TimestampCombiner.EARLIEST)
                    .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                    .discardingFiredPanes()
                    .withAllowedLateness(Duration.ZERO));
        }
        if (!rightCollection.getWindowingStrategy().isTriggerSpecified()) {
            rightCollection = rightCollection.apply("Right Collection Global Window", Window.<KV<K, R>>into(new GlobalWindows())
                    .withTimestampCombiner(TimestampCombiner.EARLIEST)
                    .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                    .discardingFiredPanes()
                    .withAllowedLateness(Duration.ZERO));
        }

        FlattenJoinPCollections flattenJoinPCollections = new FlattenJoinPCollections();
        PCollection<KV<K, RawUnionValue>> flattenedUnionTable = flattenJoinPCollections.flatten(leftCollection, rightCollection);


        PCollectionTuple joinedResult = flattenedUnionTable.apply("Join", ParDo.of(
                        new OneToOneJoinDoFn(TimeDomain.EVENT_TIME,
                                leftStateExpireDuration, rightStateExpireDuration,
                                (KvCoder<K, L>) leftCollection.getCoder(), (KvCoder<K, R>) rightCollection.getCoder()))
                .withOutputTags(outputTag, TupleTagList.of(leftTupleTag).and(rightTupleTag)));

        CoderUtil coderUtil = new CoderUtil();

        return WithDroppedJoinElements.Result.of(joinedResult, outputTag,
                leftTupleTag, rightTupleTag, KvCoder.of(coderUtil.getKeyCoder(leftCollection),
                        KvCoder.of(coderUtil.getValueCoder(leftCollection, joinType == JoinType.Right),
                                coderUtil.getValueCoder(rightCollection, joinType == JoinType.Left))));
    }


    private class OneToOneJoinDoFn extends DoFn<KV<K, RawUnionValue>,
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
        private final StateSpec<ValueState<KV<K, L>>> leftStateSpec;

        @StateId(JOINED_STATE)
        private final StateSpec<ValueState<Boolean>> joinedStateSpec;

        @StateId(RIGHT_STATE)
        private final StateSpec<ValueState<KV<K, R>>> rightStateSpec;

        private final Duration leftStateExpireDuration;
        private final Duration rightStateExpireDuration;

        private final Counter droppedRightElements;
        private final Counter droppedLeftElements;
        private final Counter joinedElements;

        public OneToOneJoinDoFn(TimeDomain timeDomain,
                                Duration leftStateExpireDuration, Duration rightStateExpireDuration,
                                KvCoder<K, L> leftCollectionCoder, KvCoder<K, R> rightCollectionCoder
        ) {
            leftStateExpiryTimerSpec = TimerSpecs.timer(timeDomain);
            rightStateExpiryTimerSpec = TimerSpecs.timer(timeDomain);
            leftStateSpec = StateSpecs.value(leftCollectionCoder);
            joinedStateSpec = StateSpecs.value(BooleanCoder.of());
            rightStateSpec = StateSpecs.value(rightCollectionCoder);
            this.leftStateExpireDuration = leftStateExpireDuration;
            this.rightStateExpireDuration = rightStateExpireDuration;

            droppedLeftElements = Metrics.counter("name", "droppedLeftElements");
            droppedRightElements = Metrics.counter("name", "droppedRightElements");
            joinedElements = Metrics.counter("name", "joinedElements");
        }

        @ProcessElement
        public void processElement(ProcessContext c,
                                   @TimerId(LEFT_STATE_EXPIRING) Timer leftStateExpiryTimer,
                                   @TimerId(RIGHT_STATE_EXPIRING) Timer rightStateExpiryTimer,
                                   @StateId(LEFT_STATE) ValueState<KV<K, L>> leftState,
                                   @StateId(JOINED_STATE) ValueState<Boolean> joinedState,
                                   @StateId(RIGHT_STATE) ValueState<KV<K, R>> rightState) {
            K key = c.element().getKey();
            RawUnionValue value = c.element().getValue();

            Optional<L> leftValue = Optional.empty();
            Optional<R> rightValue = Optional.empty();
            if (value.getValue() == null)
                return;
            if (value.getUnionTag() == 0) {
                //noinspection unchecked
                leftValue = Optional.of((L) value.getValue());
            } else {
                //noinspection unchecked
                rightValue = Optional.of((R) value.getValue());
            }
            Boolean joined = joinedState.read();
            if (joined != null && joined)
                return;
            if (!rightValue.isPresent()) {
                rightValue = Optional.ofNullable(rightState.read()).map(KV::getValue);
                if (!rightValue.isPresent()) {
                    leftState.write(KV.of(key, leftValue.get()));
                    if (c.timestamp() != null)
                        leftStateExpiryTimer.set(c.timestamp().plus(leftStateExpireDuration));
                    else
                        leftStateExpiryTimer.offset(leftStateExpireDuration).setRelative();
                    return;
                }
            }
            if (!leftValue.isPresent()) {
                leftValue = Optional.ofNullable(leftState.read()).map(KV::getValue);
                if (!leftValue.isPresent()) {
                    rightState.write(KV.of(key, rightValue.get()));
                    if (c.timestamp() != null)
                        rightStateExpiryTimer.set(c.timestamp().plus(rightStateExpireDuration));
                    else
                        rightStateExpiryTimer.offset(rightStateExpireDuration).setRelative();
                    return;
                }
            }
            KV<L, R> joinedElement = KV.of(leftValue.get(), rightValue.get());
            KV<K, KV<L, R>> keyedJoinedElement = KV.of(c.element().getKey(), joinedElement);
            joinedElements.inc();
            c.output(keyedJoinedElement);
            leftState.clear();
            rightState.clear();
            joinedState.write(true);

        }

        @OnTimer(LEFT_STATE_EXPIRING)
        public void onLeftCollectionStateExpire(OnTimerContext c,
                                                @StateId(LEFT_STATE) ValueState<KV<K, L>> leftState,
                                                @StateId(JOINED_STATE) ValueState<Boolean> joinedState) {
            Optional<KV<K, L>> leftValue = Optional.ofNullable(leftState.read());
            if (leftValue.isPresent()) {
                if (joinType == JoinType.Left) {
                    c.output(outputTag, KV.of(leftValue.get().getKey(), KV.of(leftValue.get().getValue(), null)));
                } else {
                    droppedLeftElements.inc();
                    c.output(leftTupleTag, leftValue.get().getValue());
                }
                logger.debug("Clearing Left State for {}", leftValue);
            }
            leftState.clear();
            joinedState.clear();
        }

        @OnTimer(RIGHT_STATE_EXPIRING)
        public void onRightStateExpire(OnTimerContext c,
                                       @StateId(RIGHT_STATE) ValueState<KV<K, R>> rightState,
                                       @StateId(JOINED_STATE) ValueState<Boolean> joinedState) {
            Optional<KV<K, R>> rightValue = Optional.ofNullable(rightState.read());
            if (rightValue.isPresent()) {
                if (joinType == JoinType.Right) {
                    c.output(outputTag, KV.of(rightValue.get().getKey(), KV.of(null, rightValue.get().getValue())));
                } else {
                    droppedRightElements.inc();
                    c.output(rightTupleTag, rightValue.get().getValue());
                }
                logger.debug("Clearing Right State for {}", rightValue);
            }
            rightState.clear();
            joinedState.clear();
        }

    }
}
