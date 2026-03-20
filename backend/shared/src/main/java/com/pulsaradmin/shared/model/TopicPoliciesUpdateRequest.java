package com.pulsaradmin.shared.model;

import jakarta.validation.constraints.NotBlank;

public record TopicPoliciesUpdateRequest(
    @NotBlank String topicName,
    TopicPolicies policies,
    @NotBlank String reason) {
}
