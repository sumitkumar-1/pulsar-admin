package com.pulsaradmin.shared.model;

import jakarta.validation.constraints.NotBlank;

public record ResetCursorRequest(
    @NotBlank(message = "Topic name is required.")
    String topicName,
    @NotBlank(message = "Subscription name is required.")
    String subscriptionName,
    @NotBlank(message = "Reset target is required.")
    String target,
    String timestamp,
    @NotBlank(message = "Reason is required.")
    String reason) {
}
