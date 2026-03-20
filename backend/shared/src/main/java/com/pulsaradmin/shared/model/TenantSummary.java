package com.pulsaradmin.shared.model;

public record TenantSummary(
    String name,
    int namespaceCount,
    int topicCount) {
}
