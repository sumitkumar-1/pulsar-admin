package com.pulsaradmin.shared.model;

import java.util.List;

public record TenantYamlDiffEntry(
    String action,
    String resourceType,
    String resourceName,
    String displayName,
    String summary,
    String severity,
    String iconKey,
    List<String> riskFlags,
    boolean requiresConfirmation,
    String confirmationKey,
    List<TenantYamlFieldChange> fieldChanges) {
}
