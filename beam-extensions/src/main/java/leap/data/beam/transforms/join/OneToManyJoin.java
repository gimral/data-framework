package leap.data.beam.transforms.join;

import leap.data.beam.util.CoderUtil;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.Coder;
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

public class OneToManyJoin<K, L, R> extends PTransform<PCollection<KV<K, L>>,
        WithDroppedJoinElements.Result<K, L, R>> {

    private static final Logger logger = LoggerFactory.getLogger(OneToManyJoin.class);
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

    public OneToManyJoin(PCollection<KV<K, R>> rightCollection) {
        this.rightCollection = rightCollection;
        this.leftStateExpireDuration = Duration.standardSeconds(30);
        this.rightStateExpireDuration = Duration.standardSeconds(30);
        this.joinType = JoinType.Inner;
    }

    public OneToManyJoin(PCollection<KV<K, R>> rightCollection, Duration leftStateExpireDuration, Duration rightStateExpireDuration,
                         JoinType joinType) {
        this.rightCollection = rightCollection;
        this.leftStateExpireDuration = leftStateExpireDuration;
        this.rightStateExpireDuration = rightStateExpireDuration;
        this.joinType = joinType;
    }

    public static <K, L, R> OneToManyJoin<K, L, R> inner(PCollection<KV<K, R>> rightCollection) {
        return new OneToManyJoin<>(rightCollection);
    }

    public static <K, L, R> OneToManyJoin<K, L, R> left(PCollection<KV<K, R>> rightCollection) {
        return new OneToManyJoin<K, L, R>(rightCollection).withJoinType(JoinType.Left);
    }

    public static <K, L, R> OneToManyJoin<K, L, R> fullOuter(PCollection<KV<K, R>> rightCollection) {
        return new OneToManyJoin<K, L, R>(rightCollection).withJoinType(JoinType.FullOuter);
    }

    public <L> OneToManyJoin<K, L, R> withLeftStateExpireDuration(Duration leftStateExpireDuration) {
        return new OneToManyJoin<>(this.rightCollection, leftStateExpireDuration, this.rightStateExpireDuration, this.joinType);
    }

    public <L> OneToManyJoin<K, L, R> withRightStateExpireDuration(Duration rightStateExpireDuration) {
        return new OneToManyJoin<>(this.rightCollection, this.leftStateExpireDuration, rightStateExpireDuration, this.joinType);
    }

    private OneToManyJoin<K, L, R> withJoinType(JoinType joinType) {
        return new OneToManyJoin<>(this.rightCollection, this.leftStateExpireDuration, this.rightStateExpireDuration, joinType);
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

        CoderUtil coderUtil = new CoderUtil();
        PCollectionTuple joinedResult = flattenedUnionTable.apply(joinType.name() + "Join", ParDo.of(
                        new OneToManyJoinDoFn(TimeDomain.EVENT_TIME,
                                leftStateExpireDuration, rightStateExpireDuration,
                                (KvCoder<K, L>) leftCollection.getCoder(), coderUtil.getValueCoder(rightCollection, false)))
                .withOutputTags(outputTag, TupleTagList.of(leftTupleTag).and(rightTupleTag)));


        return WithDroppedJoinElements.Result.of(joinedResult, outputTag,
                leftTupleTag, rightTupleTag, KvCoder.of(coderUtil.getKeyCoder(leftCollection),
                        KvCoder.of(coderUtil.getValueCoder(leftCollection, joinType == JoinType.FullOuter),
                                coderUtil.getValueCoder(rightCollection, joinType == JoinType.Left || joinType == JoinType.FullOuter))));

    }


    private class OneToManyJoinDoFn extends DoFn<KV<K, RawUnionValue>,
            KV<K, KV<L, R>>> {

        private static final String LEFT_STATE = "leftState";
        private static final String JOINED_STATE = "joinedState";
        private static final String RIGHT_COLLECTION_STATE = "rightCollectionState";
        private static final String KEY_STATE = "keyState";
        private static final String LEFT_STATE_EXPIRING = "leftCollectionStateExpiring";
        private static final String RIGHT_COLLECTION_STATE_EXPIRING = "rightCollectionStateExpiring";
        //        private static final String MAX_TIMESTAMP_SEEN_STATE = "maxTimeStampSeen";
        private static final String MAX_LEFT_TIMESTAMP_SEEN_STATE = "maxLeftTimeStampSeen";

        private static final String MAX_RIGHT_TIMESTAMP_SEEN_STATE = "maxRightTimeStampSeen";


        @TimerId(LEFT_STATE_EXPIRING)
        private final TimerSpec leftStateExpiryTimerSpec;

        @TimerId(RIGHT_COLLECTION_STATE_EXPIRING)
        private final TimerSpec rightCollectionStateExpiryTimerSpec;

        @StateId(LEFT_STATE)
        private final StateSpec<ValueState<KV<K, L>>> leftStateSpec;

        @StateId(JOINED_STATE)
        private final StateSpec<ValueState<Boolean>> joinedStateSpec;

        @StateId(RIGHT_COLLECTION_STATE)
        private final StateSpec<BagState<R>> rightCollectionStateSpec;

        @StateId(KEY_STATE)
        private final StateSpec<ValueState<K>> keyStateSpec;
        //
//        @StateId(MAX_TIMESTAMP_SEEN_STATE)
//        private final StateSpec<CombiningState<Long, long[], Long>> maxTimestampSeenStateSpec;
//        @StateId(MAX_LEFT_TIMESTAMP_SEEN_STATE)
//        private final StateSpec<CombiningState<Long, long[], Long>> maxLeftTimestampSeenStateSpec;
//        @StateId(MAX_RIGHT_TIMESTAMP_SEEN_STATE)
//        private final StateSpec<CombiningState<Long, long[], Long>> maxRightTimestampSeenStateSpec;
        private final Duration leftStateExpireDuration;
        private final Duration rightStateExpireDuration;
        private final Counter droppedRightElements;
        private final Counter droppedLeftElements;
        private final Counter joinedElements;
        private MaxTimestampStore maxTimestampStore;


        public OneToManyJoinDoFn(TimeDomain timeDomain,
                                 Duration leftStateExpireDuration, Duration rightStateExpireDuration,
                                 KvCoder<K, L> leftCollectionCoder, Coder<R> rightCollectionCoder
        ) {
            leftStateExpiryTimerSpec = TimerSpecs.timer(timeDomain);
            rightCollectionStateExpiryTimerSpec = TimerSpecs.timer(timeDomain);
            //keyState = StateSpecs.value(leftCollectionCoder);
            leftStateSpec = StateSpecs.value(leftCollectionCoder);
            joinedStateSpec = StateSpecs.value(BooleanCoder.of());
            rightCollectionStateSpec = StateSpecs.bag(rightCollectionCoder);
            keyStateSpec = StateSpecs.value(leftCollectionCoder.getKeyCoder());
            this.leftStateExpireDuration = leftStateExpireDuration;
            this.rightStateExpireDuration = rightStateExpireDuration;
//            maxTimestampSeenStateSpec = StateSpecs.combining(Max.ofLongs());
//            maxLeftTimestampSeenStateSpec = StateSpecs.combining(Max.ofLongs());
//            maxRightTimestampSeenStateSpec = StateSpecs.combining(Max.ofLongs());
//            maxLeftTimestampSeen = BoundedWindow.TIMESTAMP_MIN_VALUE;
//            maxRightTimestampSeen = BoundedWindow.TIMESTAMP_MIN_VALUE;
//            maxTimestampStore = new MaxTimestampStore();
            droppedLeftElements = Metrics.counter("name", "droppedLeftElements");
            droppedRightElements = Metrics.counter("name", "droppedRightElements");
            joinedElements = Metrics.counter("name", "joinedElements");
        }

        @Setup
        public void setup() {
            maxTimestampStore = new MaxTimestampStore(leftStateExpireDuration, rightStateExpireDuration);
        }

        @ProcessElement
        public void processElement(ProcessContext c,
                                   @TimerId(LEFT_STATE_EXPIRING) Timer leftStateExpiryTimer,
                                   @TimerId(RIGHT_COLLECTION_STATE_EXPIRING) Timer rightCollectionStateExpiryTimer,
                                   @StateId(LEFT_STATE) ValueState<KV<K, L>> leftState,
                                   @StateId(JOINED_STATE) ValueState<Boolean> joinedState,
                                   @StateId(RIGHT_COLLECTION_STATE) BagState<R> rightCollectionState,
                                   @StateId(KEY_STATE) ValueState<K> keyState) {
            K key = c.element().getKey();
            RawUnionValue value = c.element().getValue();

            boolean newLeftValue = false;
            boolean joined = false;
            Optional<L> leftValue = Optional.empty();
            Optional<R> rightValue = Optional.empty();
            if (value.getValue() == null)
                return;
            if (value.getUnionTag() == 0) {
                //noinspection unchecked
                leftValue = Optional.of((L) value.getValue());
                maxTimestampStore.updateLeft(c.timestamp());
                newLeftValue = true;
            } else {
                //noinspection unchecked
                rightValue = Optional.of((R) value.getValue());
                maxTimestampStore.updateRight(c.timestamp());
            }
            if (!leftValue.isPresent()) {
                leftValue = Optional.ofNullable(leftState.read()).map(KV::getValue);
            }
            //Left value exists emit all right values
            if (leftValue.isPresent()) {
                if (rightValue.isPresent()) {
                    KV<L, R> joinedElement = KV.of(leftValue.get(), rightValue.get());
                    KV<K, KV<L, R>> keyedJoinedElement = KV.of(key, joinedElement);
                    joined = true;
                    c.output(keyedJoinedElement);
                }
                //New left value is encountered emit all right elements from state
                if (newLeftValue) {
                    Iterable<R> pendingRightElements = rightCollectionState.read();
                    for (R rightElement :
                            pendingRightElements) {
                        KV<L, R> joinedElement = KV.of(leftValue.get(), rightElement);
                        KV<K, KV<L, R>> keyedJoinedElement = KV.of(key, joinedElement);
                        joined = true;
                        joinedElements.inc();
                        c.output(keyedJoinedElement);
                    }
                    rightCollectionState.clear();
                    leftState.write(KV.of(key, leftValue.get()));
//                    maxTimestampSeenState.add(c.timestamp().getMillis());
//                    Instant maxTimestamp = new Instant(maxTimestampSeenState.read());
//                    maxLeftTimestampSeenState.add(c.timestamp().getMillis());
//                    Instant timestamp = getMinimumTimestamp(maxLeftTimestampSeenState, maxRightTimestampSeenState);
                    leftStateExpiryTimer.withOutputTimestamp(c.timestamp()).set(c.timestamp().plus(leftStateExpireDuration));
                }
                if (joined) {
                    joinedState.write(true);
                    rightCollectionStateExpiryTimer.clear();
                    //maxRightTimestampSeenState.clear();
                    if (joinType == JoinType.FullOuter) {
                        keyState.clear();
                    }
                    return;
                }
            }

            if (rightValue.isPresent()) {
                rightCollectionState.add(rightValue.get());
//                maxTimestampSeenState.add(c.timestamp().getMillis());
//                Instant maxTimestamp = new Instant(maxTimestampSeenState.read());
//                maxRightTimestampSeenState.add(c.timestamp().getMillis());
//                Instant timestamp = getMinimumTimestamp(maxLeftTimestampSeenState, maxRightTimestampSeenState);
                rightCollectionStateExpiryTimer.set(c.timestamp().plus(rightStateExpireDuration));
                if (joinType == JoinType.FullOuter)
                    keyState.write(key);
            }
        }

//        private Instant getMinimumTimestamp(CombiningState<Long, long[], Long> left,
//                                            CombiningState<Long, long[], Long> right) {
//            return new Instant(Math.min(Math.max(0, left.read()), Math.max(0, right.read())));
//        }

        @OnTimer(LEFT_STATE_EXPIRING)
        public void onLeftCollectionStateExpire(OnTimerContext c,
                                                @TimerId(LEFT_STATE_EXPIRING) Timer leftStateExpiryTimer,
                                                @StateId(LEFT_STATE) ValueState<KV<K, L>> leftState,
                                                @StateId(JOINED_STATE) ValueState<Boolean> joinedState) {
            Optional<KV<K, L>> leftValue = Optional.ofNullable(leftState.read());

            System.out.println("Left timer timestamp: " + c.timestamp() + " : " + c.fireTimestamp() + ":" + leftValue.get().getKey().toString());
            if (maxTimestampStore.setLeftTimer(leftStateExpiryTimer, c)) {
                return;
            }
//            Optional<KV<K, L>> leftValue = Optional.ofNullable(leftState.read());
            //Not possible for it to be null but just to be sure
            if (leftValue.isPresent()) {
                Boolean joined = Optional.ofNullable(joinedState.read()).orElse(false);
                if (!joined) {
                    if (joinType == JoinType.Inner) {
                        droppedLeftElements.inc();
                        c.output(leftTupleTag, leftValue.get().getValue());
                    } else if (joinType == JoinType.Left || joinType == JoinType.FullOuter) {
                        c.output(outputTag, KV.of(leftValue.get().getKey(), KV.of(leftValue.get().getValue(), null)));
                    }
                }
                logger.debug("Clearing Left State for {}", leftValue);
            }
            leftState.clear();
            joinedState.clear();
//            maxLeftTimestampSeenState.clear();
        }

        @OnTimer(RIGHT_COLLECTION_STATE_EXPIRING)
        public void onRightCollectionStateExpire(OnTimerContext c,
                                                 @StateId(RIGHT_COLLECTION_STATE) BagState<R> rightCollectionState,
                                                 @StateId(KEY_STATE) ValueState<K> keyState) {
            System.out.println("Right timer timestamp: " + c.timestamp() + " : " + c.fireTimestamp());
            Iterable<R> droppedElements = rightCollectionState.read();
            for (R droppedElement :
                    droppedElements) {
                if (joinType == JoinType.Inner || joinType == JoinType.Left) {
                    droppedRightElements.inc();
                    logger.debug("Dropping from Right State for {}", droppedElement);
                    c.output(rightTupleTag, droppedElement);
                } else if (joinType == JoinType.FullOuter) {
                    c.output(outputTag, KV.of(keyState.read(), KV.of(null, droppedElement)));
                    keyState.clear();
                }
            }
            rightCollectionState.clear();
//            maxRightTimestampSeenState.clear();
        }

    }
}
