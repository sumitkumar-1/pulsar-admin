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
    int warningCount,
    List<String> warnings,
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
    return new EnvironmentSnapshot(toHealth(), tenants, namespaces, topics, warnings);
  }

  public EnvironmentSyncStatus toSyncStatus(
      String syncStatus,
      String syncMessage,
      Instant lastSyncedAt,
      Instant syncStartedAt,
      Instant lastCompletedAt) {
    return new EnvironmentSyncStatus(
        environmentId,
        syncStatus,
        syncMessage,
        lastSyncedAt,
        syncStartedAt,
        lastCompletedAt,
        tenantCount,
        namespaceCount,
        topicCount,
        warningCount,
        warnings);
  }
}
