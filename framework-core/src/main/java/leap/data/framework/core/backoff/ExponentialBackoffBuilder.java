package leap.data.framework.core.backoff;

import java.time.Duration;

public class ExponentialBackoffBuilder{

    private static final Duration DEFAULT_INITIAL_BACKOFF = Duration.ofSeconds(1);
    private static final Duration DEFAULT_MAX_BACKOFF = Duration.ofMinutes(5);
    private static final double DEFAULT_EXPONENT = 2;
    private static final int DEFAULT_MAX_RETRIES = 10;
    private static final Duration DEFAULT_MAX_CUMULATIVE_BACKOFF = Duration.ofMinutes(30);

    private Duration initialBackoff;
    private Duration maxBackoff;
    private double exponent;
    private int maxRetries;
    private Duration maxCumulativeBackoff;

    public ExponentialBackoffBuilder() {
        initialBackoff = DEFAULT_INITIAL_BACKOFF;
        maxBackoff = DEFAULT_MAX_BACKOFF;
        exponent = DEFAULT_EXPONENT;
        maxRetries = DEFAULT_MAX_RETRIES;
        maxCumulativeBackoff = DEFAULT_MAX_CUMULATIVE_BACKOFF;
    }

    public ExponentialBackoffBuilder withInitialBackoff(long initialBackoff){
        this.initialBackoff = Duration.ofMillis(initialBackoff);
        return this;
    }

    public ExponentialBackoffBuilder withMaxBackoff(long maxBackoff){
        this.maxBackoff = Duration.ofMillis(maxBackoff);
        return this;
    }

    public ExponentialBackoffBuilder withMaxCumulativeBackoff(long maxCumulativeBackoff){
        this.maxCumulativeBackoff = Duration.ofMillis(maxCumulativeBackoff);
        return this;
    }

    public ExponentialBackoffBuilder withMaxRetries(int maxRetries){
        this.maxRetries = maxRetries;
        return this;
    }

    public ExponentialBackoffBuilder withExponent(double exponent){
        this.exponent = exponent;
        return this;
    }

    public ExponentialBackoff build(){
        return new ExponentialBackoff(initialBackoff,maxBackoff,exponent,maxRetries,maxCumulativeBackoff);
    }
}
