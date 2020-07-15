package leap.data.beam.window;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.*;

public class CustomSessions extends WindowFn<Object, IntervalWindow> {
    /**
     * Duration of the gaps between sessions.
     */
    private final Duration gapDuration;
    private final Duration maxDuration;

    /**
     * Creates a {@code Sessions} {@link WindowFn} with the specified gap duration.
     */
    public static CustomSessions withGapAndMaxDuration(Duration gapDuration, Duration maxDuration) {
        return new CustomSessions(gapDuration, maxDuration);
    }

    /**
     * Creates a {@code Sessions} {@link WindowFn} with the specified gap duration.
     */
    private CustomSessions(Duration gapDuration, Duration maxDuration) {
        this.gapDuration = gapDuration;
        this.maxDuration = maxDuration;
    }

    @Override
    public Collection<IntervalWindow> assignWindows(AssignContext c) {
        // Assign each element into a window from its timestamp until gapDuration in the
        // future.  Overlapping windows (representing elements within gapDuration of
        // each other) will be merged.
        return Arrays.asList(new IntervalWindow(c.timestamp(), gapDuration));
    }

    @Override
    public void mergeWindows(MergeContext c) throws Exception {
        List<IntervalWindow> sortedWindows = new ArrayList<>();
        for (IntervalWindow window : c.windows()) {
            sortedWindows.add(window);
        }
        Collections.sort(sortedWindows);
        List<MergeCandidate> merges = new ArrayList<>();
        MergeCandidate current = new MergeCandidate();
        for (IntervalWindow window : sortedWindows) {
            if (window.start().plus(maxDuration).isBefore(window.end()) &
                    current.intersects(window)) {
                current.add(window);
            } else {
                merges.add(current);
                current = new MergeCandidate(window);
            }
        }
        merges.add(current);
        for (MergeCandidate merge : merges) {
            merge.apply(c);
        }
    }

    @Override
    public Coder<IntervalWindow> windowCoder() {
        return IntervalWindow.getCoder();
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
        return other instanceof org.apache.beam.sdk.transforms.windowing.Sessions;
    }

    @Override
    public void verifyCompatibility(WindowFn<?, ?> other) throws IncompatibleWindowException {
        if (!this.isCompatible(other)) {
            throw new IncompatibleWindowException(
                    other,
                    String.format(
                            "%s is only compatible with %s.",
                            org.apache.beam.sdk.transforms.windowing.Sessions.class.getSimpleName(), org.apache.beam.sdk.transforms.windowing.Sessions.class.getSimpleName()));
        }
    }

    @Override
    public TypeDescriptor<IntervalWindow> getWindowTypeDescriptor() {
        return TypeDescriptor.of(IntervalWindow.class);
    }

    @Override
    public WindowMappingFn<IntervalWindow> getDefaultWindowMappingFn() {
        throw new UnsupportedOperationException("Sessions is not allowed in side inputs");
    }

    public Duration getGapDuration() {
        return gapDuration;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);
        builder.add(DisplayData.item("gapDuration", gapDuration).withLabel("Session Gap Duration"));
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof org.apache.beam.sdk.transforms.windowing.Sessions)) {
            return false;
        }
        org.apache.beam.sdk.transforms.windowing.Sessions other = (org.apache.beam.sdk.transforms.windowing.Sessions) object;
        return getGapDuration().equals(other.getGapDuration());
    }

    @Override
    public int hashCode() {
        return Objects.hash(gapDuration);
    }

    private static class MergeCandidate {
        @Nullable
        private IntervalWindow union;
        private final List<IntervalWindow> parts;

        public MergeCandidate() {
            union = null;
            parts = new ArrayList<>();
        }

        public MergeCandidate(IntervalWindow window) {
            union = window;
            parts = new ArrayList<>(Arrays.asList(window));
        }

        public boolean intersects(IntervalWindow window) {
            return union == null || union.intersects(window);
        }

        public void add(IntervalWindow window) {
            union = union == null ? window : union.span(window);
            parts.add(window);
        }

        public void apply(WindowFn<?, IntervalWindow>.MergeContext c) throws Exception {
            if (parts.size() > 1) {
                c.merge(parts, union);
            }
        }

        @Override
        public String toString() {
            return "MergeCandidate[union=" + union + ", parts=" + parts + "]";
        }
    }
}
