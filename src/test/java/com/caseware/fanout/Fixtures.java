package com.caseware.fanout;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.stream.Stream;

/** Small fakes shared by the tests. */
final class Fixtures {
    private Fixtures() {}

    static final class ManualClock extends Clock {
        private volatile Instant now = Instant.parse("2026-11-12T10:00:00Z");

        void advance(Duration d) {
            now = now.plus(d);
        }

        @Override public Instant instant() { return now; }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
    }

    /** Monotonic index, as the real one must be. */
    static final class FakeIndex implements VersionIndex {
        record Entry(int version, long revision) {}

        final Map<String, Entry> entries = new ConcurrentHashMap<>();

        @Override
        public void recordVerified(String engagementId, int templateVersion, long revision) {
            entries.merge(engagementId, new Entry(templateVersion, revision),
                    (old, fresh) -> fresh.revision() > old.revision() ? fresh : old);
        }
    }

    /** Returns the engagements the index doesn't know yet, like the real directory. */
    static final class FakeDirectory implements EngagementDirectory {
        private final List<EngagementRef> engagements = new CopyOnWriteArrayList<>();
        private final FakeIndex index;

        FakeDirectory(FakeIndex index) {
            this.index = index;
        }

        FakeDirectory add(String tenantId, int count) {
            int start = engagements.size();
            for (int i = 0; i < count; i++) {
                engagements.add(new EngagementRef(tenantId, tenantId + "-" + (start + i)));
            }
            return this;
        }

        @Override
        public Stream<EngagementRef> unverifiedEngagements(String templateId) {
            return engagements.stream().filter(e -> !index.entries.containsKey(e.engagementId()));
        }
    }

    /** Downstream driven by a function; records every call. */
    static final class FakeClient implements VerificationClient {
        final List<EngagementRef> calls = new CopyOnWriteArrayList<>();
        volatile Function<EngagementRef, VerificationResult> behaviour = e -> new VerificationResult.Verified(5, 1);

        @Override
        public VerificationResult verify(EngagementRef engagement, Duration timeout) {
            calls.add(engagement);
            return behaviour.apply(engagement);
        }
    }
}
