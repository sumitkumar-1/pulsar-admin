package com.pulsaradmin.shared.model;

import java.util.List;

public record TenantYamlPreviewResponse(
    String previewId,
    String environmentId,
    String tenant,
    String namespace,
    boolean valid,
    String message,
    List<String> errors,
    int totalCreates,
    int totalUpdates,
    int totalRemovals,
    int dangerousRemovals,
    int blockedChanges,
    List<String> requiredConfirmations,
    List<TenantYamlDiffEntry> changes) {
}
