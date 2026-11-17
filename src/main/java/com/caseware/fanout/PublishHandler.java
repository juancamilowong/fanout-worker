package com.caseware.fanout;

import java.time.Clock;
import java.util.stream.Stream;

/**
 * Turns a publish into verification work. Fast: it only reads the index and writes queue rows; the slow
 * calls happen in {@link FanoutWorker}.
 *
 * <p>Idempotent: a redelivered or out-of-order event finds the work already queued (no-op) or the
 * engagements already verified (not returned by the directory).
 */
public final class PublishHandler {

    private final EngagementDirectory directory;
    private final WorkQueue queue;
    private final Clock clock;

    public PublishHandler(EngagementDirectory directory, WorkQueue queue, Clock clock) {
        this.directory = directory;
        this.queue = queue;
        this.clock = clock;
    }

    /** @return number of engagements newly queued */
    public int onTemplatePublished(TemplatePublished event) {
        try (Stream<EngagementRef> engagements = directory.unverifiedEngagements(event.templateId())) {
            return (int) engagements.filter(e -> queue.enqueue(e, clock.instant())).count();
        }
    }
}
