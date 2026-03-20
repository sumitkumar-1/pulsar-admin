package com.pulsaradmin.shared.model;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record ConsumeMessagesRequest(
    @NotBlank String topicName,
    String subscriptionName,
    boolean ephemeral,
    @Min(1) @Max(50) int maxMessages,
    @Min(1) @Max(30) int waitTimeSeconds,
    @NotBlank String reason) {
}
