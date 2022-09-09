package leap.data.beam.transforms.join;

import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class MaxTimestampStore {
    private static final Duration maxNextDuration = Duration.standardMinutes(10);
    private static final Duration minNextDuration = Duration.standardMinutes(1);

    private static Instant maxLeftTimestampSeen = BoundedWindow.TIMESTAMP_MIN_VALUE;
    private static Instant maxRightTimestampSeen = BoundedWindow.TIMESTAMP_MIN_VALUE;
    private final Duration leftStateExpireDuration;
    private final Duration rightStateExpireDuration;

    public MaxTimestampStore(Duration leftStateExpireDuration, Duration rightStateExpireDuration) {
        this.leftStateExpireDuration = leftStateExpireDuration;
        this.rightStateExpireDuration = rightStateExpireDuration;
    }

    public void updateLeft(Instant value) {
        maxLeftTimestampSeen = maxLeftTimestampSeen.getMillis() < value.getMillis() ? value : maxLeftTimestampSeen;
    }

    public void updateRight(Instant value) {
        maxRightTimestampSeen = maxRightTimestampSeen.getMillis() < value.getMillis() ? value : maxRightTimestampSeen;
    }

    public <I, O> boolean setLeftTimer(Timer leftStateExpiryTimer, DoFn<I, O>.OnTimerContext c) {
        if (c.timestamp().plus(leftStateExpireDuration).getMillis() > maxRightTimestampSeen.getMillis()) {
            leftStateExpiryTimer.withOutputTimestamp(c.timestamp()).set(getNextTime(c.fireTimestamp(), maxRightTimestampSeen));
            return true;
        }
        return false;
    }

    public <I, O> boolean setRightTimer(Timer rightStateExpiryTimer, DoFn<I, O>.OnTimerContext c) {
        if (c.timestamp().plus(rightStateExpireDuration).getMillis() > maxLeftTimestampSeen.getMillis()) {
            rightStateExpiryTimer.withOutputTimestamp(c.timestamp()).set(getNextTime(c.fireTimestamp(), maxLeftTimestampSeen));
            return true;
        }
        return false;
    }

    private Instant getNextTime(Instant fireTimestamp, Instant maxTimestampSeen) {
        return fireTimestamp.plus(
                Math.min(maxNextDuration.getMillis(),
                        Math.max(minNextDuration.getMillis(), fireTimestamp.getMillis() - maxTimestampSeen.getMillis())));
    }


}
