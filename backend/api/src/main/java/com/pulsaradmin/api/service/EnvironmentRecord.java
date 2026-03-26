package com.pulsaradmin.api.service;

import com.pulsaradmin.shared.model.EnvironmentDetails;
import com.pulsaradmin.shared.model.EnvironmentStatus;
import com.pulsaradmin.shared.model.EnvironmentSummary;
import java.time.Instant;

public record EnvironmentRecord(
    String id,
    String name,
    String kind,
    String region,
    String clusterLabel,
    String summary,
    String brokerUrl,
    String adminUrl,
    String authMode,
    String credentialReference,
    String syncTargets,
    boolean tlsEnabled,
    EnvironmentStatus status,
    String syncStatus,
    String syncMessage,
    Instant lastSyncedAt,
    String lastTestStatus,
    String lastTestMessage,
    Instant lastTestedAt,
    Instant deletedAt) {

  public EnvironmentSummary toSummary() {
    return new EnvironmentSummary(
        id,
        name,
        kind,
        status,
        region,
        clusterLabel,
        summary,
        syncStatus,
        lastSyncedAt,
        lastTestStatus);
  }

  public EnvironmentDetails toDetails() {
    return new EnvironmentDetails(
        id,
        name,
        kind,
        status,
        region,
        clusterLabel,
        summary,
        brokerUrl,
        adminUrl,
        authMode,
        credentialReference,
        syncTargets,
        tlsEnabled,
        syncStatus,
        syncMessage,
        lastSyncedAt,
        lastTestStatus,
        lastTestMessage,
        lastTestedAt,
        deletedAt != null);
  }
}
