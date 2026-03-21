package com.pulsaradmin.shared.model;

public record NamespaceMutationResponse(
    String environmentId,
    String tenant,
    String namespace,
    String action,
    String message,
    CatalogSummary catalogSummary) {
}
