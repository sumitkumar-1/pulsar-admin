package com.pulsaradmin.shared.model;

public record TopicPoliciesResponse(
    String environmentId,
    String topicName,
    TopicPolicies policies,
    boolean editable,
    String message) {
}
