package com.pulsaradmin.shared.model;

public record UnloadTopicResponse(
    String environmentId,
    String topicName,
    String message,
    TopicDetails topicDetails) {
}
