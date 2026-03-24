package com.pulsaradmin.shared.model;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record ReplayCopyJobRequest(
    @NotBlank(message = "Topic name is required.")
    String topicName,
    @NotBlank(message = "Subscription name is required.")
    String subscriptionName,
    String operation,
    String operationMode,
    String destinationTopicName,
    @Min(value = 1, message = "Message limit must be at least 1.")
    @Max(value = 1_000_000, message = "Message limit must be 1000000 or less.")
    int messageLimit,
    String messageKey,
    java.util.Map<String, String> propertyFilters,
    String filterText,
    String matchField,
    Boolean autoReplicateSchema,
    @Min(value = 1, message = "Rate limit must be at least 1.")
    @Max(value = 5000, message = "Rate limit must be 5000 or less.")
    int messagesPerSecond,
    @NotBlank(message = "Reason is required.")
    String reason) {
}
