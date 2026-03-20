package com.pulsaradmin.shared.model;

import java.util.List;

public record TenantYamlApplyResponse(
    String previewId,
    String environmentId,
    String tenant,
    String message,
    List<TenantYamlDiffEntry> appliedChanges,
    CatalogSummary catalogSummary) {
}
