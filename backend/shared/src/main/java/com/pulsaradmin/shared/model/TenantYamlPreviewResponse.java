package com.pulsaradmin.shared.model;

import java.util.List;

public record TenantYamlPreviewResponse(
    String previewId,
    String environmentId,
    String tenant,
    boolean valid,
    String message,
    List<String> errors,
    List<TenantYamlDiffEntry> changes) {
}
