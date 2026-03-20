package com.pulsaradmin.shared.model;

public record CatalogMutationResponse(
    String environmentId,
    String resourceType,
    String resourceName,
    String message,
    CatalogSummary catalogSummary) {
}
