package com.pulsaradmin.shared.model;

public record TopicDeleteResponse(
    String environmentId,
    String topicName,
    String tenant,
    String namespace,
    String message,
    CatalogSummary catalogSummary) {
}
