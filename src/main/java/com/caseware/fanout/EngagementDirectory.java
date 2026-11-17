package com.caseware.fanout;

import java.util.stream.Stream;

/** Reads the regional version index. Never loads an engagement. */
public interface EngagementDirectory {

    /**
     * Engagements on this template whose indexed version is unknown or stale. Verified engagements are
     * excluded: their lifecycle hooks already keep the index current, so they need no downstream call.
     * Streamed, because a large product can have tens of thousands.
     */
    Stream<EngagementRef> unverifiedEngagements(String templateId);
}
