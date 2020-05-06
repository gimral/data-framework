package leap.data.framework.core.backoff;

import java.time.Duration;

public class ExponentialBackoff {
    private final Duration initialBackoff;
    private final Duration maxBackoff;
    private final double exponent;
    private final int maxRetries;
    private final Duration maxCumulativeBackoff;

    private int currentRetries;
    private Duration currentCumulativeBackoff;

    public ExponentialBackoff(Duration initialBackoff, Duration maxBackoff, double exponent, int maxRetries, Duration maxCumulativeBackoff) {
        this.initialBackoff = initialBackoff;
        this.maxBackoff = maxBackoff;
        this.exponent = exponent;
        this.maxRetries = maxRetries;
        this.maxCumulativeBackoff = maxCumulativeBackoff;

        reset();
    }

    public void reset(){
        currentRetries = 0;
        currentCumulativeBackoff = Duration.ZERO;
    }

    public Duration nextWait(){
        if(currentRetries >= maxRetries || currentCumulativeBackoff.toMillis() >= maxCumulativeBackoff.toMillis() )
            return Duration.ZERO;
        double maxRemainingBackoff = Math.min(maxCumulativeBackoff.minus(currentCumulativeBackoff).toMillis(),maxBackoff.toMillis());
        long nextBackOff =  Math.round(Math.min(maxRemainingBackoff,initialBackoff.toMillis() * Math.pow(exponent, currentRetries)));
        //TODO: Add randomization
        maxCumulativeBackoff.plus(Duration.ofMillis(nextBackOff));
        currentRetries++;
        return Duration.ofMillis(nextBackOff);
    }

    public ExponentialBackoffBuilder builder(){
        return new ExponentialBackoffBuilder();
    }
}

