package com.pulsaradmin.shared.model;

public record NamespaceSummary(
    String tenant,
    String namespace,
    int topicCount) {
}
