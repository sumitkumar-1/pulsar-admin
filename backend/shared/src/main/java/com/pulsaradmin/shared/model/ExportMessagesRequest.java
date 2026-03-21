package com.pulsaradmin.shared.model;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record ExportMessagesRequest(
    @NotBlank String topicName,
    @NotBlank String source,
    String subscriptionName,
    boolean ephemeral,
    @Min(1) @Max(25) int maxMessages,
    @Min(1) @Max(30) int waitTimeSeconds,
    @NotBlank String reason) {
}
