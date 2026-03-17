package com.pulsaradmin.shared.model;

public record SchemaSummary(
    String type,
    String version,
    String compatibility,
    boolean present) {
}
