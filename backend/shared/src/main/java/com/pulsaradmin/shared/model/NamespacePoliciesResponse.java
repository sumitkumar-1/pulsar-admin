package com.pulsaradmin.shared.model;

public record NamespacePoliciesResponse(
    String environmentId,
    String tenant,
    String namespace,
    NamespacePolicies policies,
    String message,
    NamespaceDetails namespaceDetails) {
}
