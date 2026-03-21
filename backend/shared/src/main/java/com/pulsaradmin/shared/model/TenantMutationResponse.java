package com.pulsaradmin.shared.model;

public record TenantMutationResponse(
    String environmentId,
    String tenant,
    String action,
    String message,
    TenantDetails tenantDetails,
    CatalogSummary catalogSummary) {
}
