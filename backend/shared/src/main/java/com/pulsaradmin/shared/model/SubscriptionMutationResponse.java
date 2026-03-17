package com.pulsaradmin.shared.model;

public record SubscriptionMutationResponse(
    String environmentId,
    String topicName,
    String subscriptionName,
    String action,
    String initialPosition,
    String message,
    TopicDetails topicDetails) {
}
