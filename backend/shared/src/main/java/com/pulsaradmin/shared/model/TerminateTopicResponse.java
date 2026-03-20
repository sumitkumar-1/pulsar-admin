package com.pulsaradmin.shared.model;

public record TerminateTopicResponse(
    String environmentId,
    String topicName,
    String lastMessageId,
    String message,
    TopicDetails topicDetails) {
}
