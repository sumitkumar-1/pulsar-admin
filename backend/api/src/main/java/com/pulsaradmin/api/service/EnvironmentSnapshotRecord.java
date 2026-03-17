package com.pulsaradmin.api.service;

import com.pulsaradmin.shared.model.EnvironmentHealth;
import com.pulsaradmin.shared.model.EnvironmentSnapshot;
import com.pulsaradmin.shared.model.EnvironmentSyncStatus;
import com.pulsaradmin.shared.model.EnvironmentStatus;
import com.pulsaradmin.shared.model.TopicDetails;
import java.time.Instant;
import java.util.List;

public record EnvironmentSnapshotRecord(
    String environmentId,
    EnvironmentStatus healthStatus,
    String healthMessage,
    String pulsarVersion,
    String brokerUrl,
    String adminUrl,
    int tenantCount,
    int namespaceCount,
    int topicCount,
    List<String> tenants,
    List<String> namespaces,
    List<TopicDetails> topics,
    Instant updatedAt) {

  public EnvironmentHealth toHealth() {
    return new EnvironmentHealth(
        environmentId,
        healthStatus,
        brokerUrl,
        adminUrl,
        pulsarVersion,
        healthMessage);
  }

  public EnvironmentSnapshot toSnapshot() {
    return new EnvironmentSnapshot(toHealth(), tenants, namespaces, topics);
  }

  public EnvironmentSyncStatus toSyncStatus(String syncStatus, String syncMessage, Instant lastSyncedAt) {
    return new EnvironmentSyncStatus(environmentId, syncStatus, syncMessage, lastSyncedAt, tenantCount, namespaceCount, topicCount);
  }
}
