package com.pulsaradmin.shared.model;

public record TenantYamlDiffEntry(
    String action,
    String resourceType,
    String resourceName,
    String summary) {
}
