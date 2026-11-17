package com.caseware.fanout;

/** A template version was published. Delivered at least once, possibly out of order. */
public record TemplatePublished(String eventId, String templateId, int version) {}
