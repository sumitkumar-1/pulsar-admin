package com.pulsaradmin.shared.model;

import java.util.List;

public record CatalogSummary(
    String environmentId,
    List<TenantSummary> tenants,
    List<NamespaceSummary> namespaces) {
}
