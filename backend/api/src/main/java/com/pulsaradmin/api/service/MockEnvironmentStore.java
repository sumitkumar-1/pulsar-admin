package com.pulsaradmin.api.service;

import com.pulsaradmin.api.support.NotFoundException;
import com.pulsaradmin.shared.model.EnvironmentSnapshot;
import com.pulsaradmin.shared.model.EnvironmentStatus;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.stereotype.Component;

@Component
public class MockEnvironmentStore {
  private final ConcurrentMap<String, EnvironmentRecord> environments = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, EnvironmentSnapshotRecord> snapshots = new ConcurrentHashMap<>();

  public MockEnvironmentStore() {
    seedDefaults();
  }

  public List<EnvironmentRecord> findAllActive() {
    return environments.values().stream()
        .filter(record -> record.deletedAt() == null)
        .sorted(Comparator.comparing(EnvironmentRecord::name))
        .toList();
  }

  public EnvironmentRecord findActiveById(String environmentId) {
    EnvironmentRecord record = environments.get(environmentId);
    if (record == null || record.deletedAt() != null) {
      throw new NotFoundException("Unknown environment: " + environmentId);
    }
    return record;
  }

  public boolean existsActiveById(String environmentId) {
    EnvironmentRecord record = environments.get(environmentId);
    return record != null && record.deletedAt() == null;
  }

  public void insert(EnvironmentRecord record) {
    environments.put(record.id(), record);
  }

  public void update(EnvironmentRecord record) {
    environments.put(record.id(), record);
  }

  public void softDelete(String environmentId) {
    EnvironmentRecord existing = findActiveById(environmentId);
    environments.put(environmentId, new EnvironmentRecord(
        existing.id(),
        existing.name(),
        existing.kind(),
        existing.region(),
        existing.clusterLabel(),
        existing.summary(),
        existing.brokerUrl(),
        existing.adminUrl(),
        existing.authMode(),
        existing.credentialReference(),
        existing.syncTargets(),
        existing.tlsEnabled(),
        existing.status(),
        existing.syncStatus(),
        existing.syncMessage(),
        existing.lastSyncedAt(),
        existing.lastTestStatus(),
        existing.lastTestMessage(),
        existing.lastTestedAt(),
        Instant.now()));
    snapshots.remove(environmentId);
  }

  public EnvironmentSnapshotRecord storeSnapshot(String environmentId, EnvironmentSnapshot snapshot) {
    EnvironmentSnapshotRecord record = new EnvironmentSnapshotRecord(
        environmentId,
        snapshot.health().status(),
        snapshot.health().message(),
        snapshot.health().pulsarVersion(),
        snapshot.health().brokerUrl(),
        snapshot.health().adminUrl(),
        snapshot.tenants().size(),
        snapshot.namespaces().size(),
        snapshot.topics().size(),
        new ArrayList<>(snapshot.tenants()),
        new ArrayList<>(snapshot.namespaces()),
        new ArrayList<>(snapshot.topics()),
        Instant.now());
    snapshots.put(environmentId, record);
    return record;
  }

  public EnvironmentSnapshotRecord getSnapshot(String environmentId) {
    return snapshots.get(environmentId);
  }

  public void clearSnapshot(String environmentId) {
    snapshots.remove(environmentId);
  }

  private void seedDefaults() {
    Instant now = Instant.now();
    insert(new EnvironmentRecord(
        "prod",
        "Production",
        "prod",
        "us-east-1",
        "cluster-a",
        "Primary customer traffic",
        "pulsar+ssl://prod-brokers:6651",
        "https://prod-admin.internal",
        "token",
        "env://PULSAR_PROD_TOKEN",
        null,
        true,
        EnvironmentStatus.HEALTHY,
        "SYNCED",
        "Demo metadata is ready.",
        now,
        "SUCCESS",
        "Mock demo connection verified.",
        now,
        null));
    insert(new EnvironmentRecord(
        "stable",
        "Stable",
        "stable",
        "us-east-1",
        "cluster-b",
        "Pre-release validation",
        "pulsar+ssl://stable-brokers:6651",
        "https://stable-admin.internal",
        "token",
        "env://PULSAR_STABLE_TOKEN",
        null,
        true,
        EnvironmentStatus.HEALTHY,
        "SYNCED",
        "Demo metadata is ready.",
        now,
        "SUCCESS",
        "Mock demo connection verified.",
        now,
        null));
    insert(new EnvironmentRecord(
        "qa",
        "QA",
        "qa",
        "us-east-2",
        "cluster-c",
        "Regression and release testing",
        "pulsar://qa-brokers:6650",
        "https://qa-admin.internal",
        "basic",
        "env://PULSAR_QA_BASIC",
        null,
        false,
        EnvironmentStatus.DEGRADED,
        "SYNCED",
        "Demo metadata is ready.",
        now,
        "SUCCESS",
        "Mock demo connection verified.",
        now,
        null));
    insert(new EnvironmentRecord(
        "dev",
        "Development",
        "dev",
        "local",
        "cluster-dev",
        "Everyday engineering workflows",
        "pulsar://dev-brokers:6650",
        "https://dev-admin.internal",
        "none",
        null,
        null,
        false,
        EnvironmentStatus.HEALTHY,
        "SYNCED",
        "Demo metadata is ready.",
        now,
        "SUCCESS",
        "Mock demo connection verified.",
        now,
        null));
  }
}
