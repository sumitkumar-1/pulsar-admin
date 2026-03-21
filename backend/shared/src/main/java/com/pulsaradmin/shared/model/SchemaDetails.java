package com.pulsaradmin.shared.model;

public record SchemaDetails(
    String environmentId,
    String topicName,
    boolean present,
    String type,
    String version,
    String compatibility,
    String definition,
    boolean editable,
    String message) {
}
