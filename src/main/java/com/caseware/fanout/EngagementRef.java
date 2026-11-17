package com.caseware.fanout;

/** An engagement and the firm (tenant) that owns it. */
public record EngagementRef(String tenantId, String engagementId) {}
