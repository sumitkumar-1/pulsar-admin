package com.pulsaradmin.shared.model;

public record TenantYamlFieldChange(
    String field,
    String currentValue,
    String desiredValue) {
}
