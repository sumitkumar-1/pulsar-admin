package com.pulsaradmin.shared.model;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record ReplayCopyJobRequest(
    @NotBlank(message = "Topic name is required.")
    String topicName,
    @NotBlank(message = "Subscription name is required.")
    String subscriptionName,
    @NotBlank(message = "Operation is required.")
    String operation,
    @NotBlank(message = "Destination topic name is required.")
    String destinationTopicName,
    @Min(value = 1, message = "Message limit must be at least 1.")
    @Max(value = 5000, message = "Message limit must be 5000 or less.")
    int messageLimit,
    String filterText,
    @Min(value = 1, message = "Rate limit must be at least 1.")
    @Max(value = 1000, message = "Rate limit must be 1000 or less.")
    int messagesPerSecond,
    @NotBlank(message = "Reason is required.")
    String reason) {
}
