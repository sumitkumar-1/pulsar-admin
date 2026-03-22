package com.pulsaradmin.shared.model;

public record TenantYamlApplyResultEntry(
    String action,
    String resourceType,
    String resourceName,
    String status,
    String message) {
}
