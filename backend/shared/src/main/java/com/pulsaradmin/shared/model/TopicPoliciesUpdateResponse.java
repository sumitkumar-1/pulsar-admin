package com.pulsaradmin.shared.model;

public record TopicPoliciesUpdateResponse(
    String environmentId,
    String topicName,
    TopicPolicies policies,
    String message,
    TopicDetails topicDetails) {
}
