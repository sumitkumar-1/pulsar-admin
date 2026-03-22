package com.pulsaradmin.shared.model;

import java.util.List;

public record TenantYamlApplyResponse(
    String previewId,
    String environmentId,
    String tenant,
    String namespace,
    String message,
    List<TenantYamlDiffEntry> appliedChanges,
    List<TenantYamlApplyResultEntry> applyResults,
    int appliedCount,
    int skippedCount,
    int failedCount,
    CatalogSummary catalogSummary) {
}
