package com.pulsaradmin.shared.model;

public record SchemaMutationResponse(
    String environmentId,
    String topicName,
    String action,
    String message,
    SchemaDetails schema,
    TopicDetails topicDetails) {
}
