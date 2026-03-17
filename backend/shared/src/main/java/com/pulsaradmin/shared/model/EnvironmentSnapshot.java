package com.pulsaradmin.shared.model;

import java.util.List;

public record EnvironmentSnapshot(
    EnvironmentHealth health,
    List<String> tenants,
    List<String> namespaces,
    List<TopicDetails> topics) {
}
