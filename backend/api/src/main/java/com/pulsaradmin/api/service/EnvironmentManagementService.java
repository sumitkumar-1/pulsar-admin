package com.pulsaradmin.api.service;

import com.pulsaradmin.api.support.BadRequestException;
import com.pulsaradmin.api.support.NotFoundException;
import com.pulsaradmin.shared.gateway.PulsarAdminGateway;
import com.pulsaradmin.shared.model.EnvironmentConnectionTestResult;
import com.pulsaradmin.shared.model.EnvironmentDetails;
import com.pulsaradmin.shared.model.EnvironmentSnapshot;
import com.pulsaradmin.shared.model.EnvironmentStatus;
import com.pulsaradmin.shared.model.EnvironmentSummary;
import com.pulsaradmin.shared.model.EnvironmentSyncStatus;
import com.pulsaradmin.shared.model.EnvironmentUpsertRequest;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.springframework.stereotype.Service;

@Service
public class EnvironmentManagementService {
  private static final Set<String> SUPPORTED_AUTH_MODES = Set.of("none", "token", "basic", "mtls");
  private final EnvironmentRepository environmentRepository;
  private final EnvironmentSnapshotRepository snapshotRepository;
  private final PulsarAdminGateway pulsarAdminGateway;
  private final GatewayModeResolver gatewayModeResolver;
  private final MockEnvironmentStore mockEnvironmentStore;

  public EnvironmentManagementService(
      EnvironmentRepository environmentRepository,
      EnvironmentSnapshotRepository snapshotRepository,
      PulsarAdminGateway pulsarAdminGateway,
      GatewayModeResolver gatewayModeResolver,
      MockEnvironmentStore mockEnvironmentStore) {
    this.environmentRepository = environmentRepository;
    this.snapshotRepository = snapshotRepository;
    this.pulsarAdminGateway = pulsarAdminGateway;
    this.gatewayModeResolver = gatewayModeResolver;
    this.mockEnvironmentStore = mockEnvironmentStore;
  }

  public List<EnvironmentSummary> getEnvironments() {
    return activeEnvironmentRecords().stream()
        .map(EnvironmentRecord::toSummary)
        .toList();
  }

  public EnvironmentDetails getEnvironment(String environmentId) {
    return requireEnvironment(environmentId).toDetails();
  }

  public EnvironmentDetails createEnvironment(EnvironmentUpsertRequest request) {
    if (activeEnvironmentExists(request.id())) {
      throw new BadRequestException("Environment id already exists: " + request.id());
    }

    validateRequest(request);

    EnvironmentRecord environment = new EnvironmentRecord(
        request.id(),
        request.name(),
        request.kind(),
        request.region(),
        request.clusterLabel(),
        request.summary(),
        request.brokerUrl(),
        request.adminUrl(),
        request.authMode(),
        blankToNull(request.credentialReference()),
        request.tlsEnabled(),
        EnvironmentStatus.DEGRADED,
        "NOT_SYNCED",
        "Environment created. Run connection test to sync metadata.",
        null,
        "NOT_TESTED",
        "Connection has not been tested yet.",
        null,
        null);

    if (isMockMode()) {
      mockEnvironmentStore.insert(environment);
      mockEnvironmentStore.clearSnapshot(environment.id());
    } else {
      if (environmentRepository.findById(request.id()).isPresent()) {
        environmentRepository.update(environment);
      } else {
        environmentRepository.insert(environment);
      }
    }
    return environment.toDetails();
  }

  public EnvironmentDetails updateEnvironment(String environmentId, EnvironmentUpsertRequest request) {
    if (!environmentId.equals(request.id())) {
      throw new BadRequestException("Environment id cannot be changed.");
    }

    validateRequest(request);

    EnvironmentRecord existing = requireEnvironment(environmentId);

    EnvironmentRecord updated = new EnvironmentRecord(
        existing.id(),
        request.name(),
        request.kind(),
        request.region(),
        request.clusterLabel(),
        request.summary(),
        request.brokerUrl(),
        request.adminUrl(),
        request.authMode(),
        blankToNull(request.credentialReference()),
        request.tlsEnabled(),
        existing.status(),
        "NOT_SYNCED",
        "Environment updated. Re-test the connection to resync metadata.",
        existing.lastSyncedAt(),
        "NOT_TESTED",
        "Connection needs to be re-tested after the latest update.",
        existing.lastTestedAt(),
        existing.deletedAt());

    if (isMockMode()) {
      mockEnvironmentStore.update(updated);
      mockEnvironmentStore.clearSnapshot(updated.id());
    } else {
      environmentRepository.update(updated);
    }
    return updated.toDetails();
  }

  public EnvironmentConnectionTestResult testConnection(String environmentId) {
    EnvironmentRecord environment = requireEnvironment(environmentId);
    EnvironmentConnectionTestResult result = pulsarAdminGateway.testConnection(environment.toDetails());
    Instant testedAt = result.testedAt();

    EnvironmentRecord testedEnvironment = new EnvironmentRecord(
        environment.id(),
        environment.name(),
        environment.kind(),
        environment.region(),
        environment.clusterLabel(),
        environment.summary(),
        environment.brokerUrl(),
        environment.adminUrl(),
        environment.authMode(),
        environment.credentialReference(),
        environment.tlsEnabled(),
        result.successful() ? EnvironmentStatus.HEALTHY : EnvironmentStatus.DEGRADED,
        environment.syncStatus(),
        environment.syncMessage(),
        environment.lastSyncedAt(),
        result.status(),
        result.message(),
        testedAt,
        environment.deletedAt());

    updateEnvironmentRecord(testedEnvironment);

    if (result.successful()) {
      syncEnvironment(environmentId);
      return new EnvironmentConnectionTestResult(environmentId, true, result.status(), result.message(), testedAt, true);
    }

    return result;
  }

  public EnvironmentSyncStatus syncEnvironment(String environmentId) {
    EnvironmentRecord environment = requireEnvironment(environmentId);

    if (!"SUCCESS".equalsIgnoreCase(environment.lastTestStatus())) {
      throw new BadRequestException("Run a successful connection test before syncing this environment.");
    }

    EnvironmentSnapshot snapshot = pulsarAdminGateway.syncMetadata(environment.toDetails());
    EnvironmentSnapshotRecord snapshotRecord = storeSnapshot(environmentId, snapshot);

    Instant syncedAt = Instant.now();
    EnvironmentRecord syncedEnvironment = new EnvironmentRecord(
        environment.id(),
        environment.name(),
        environment.kind(),
        environment.region(),
        environment.clusterLabel(),
        environment.summary(),
        environment.brokerUrl(),
        environment.adminUrl(),
        environment.authMode(),
        environment.credentialReference(),
        environment.tlsEnabled(),
        snapshot.health().status(),
        "SYNCED",
        "Metadata synced successfully.",
        syncedAt,
        environment.lastTestStatus(),
        environment.lastTestMessage(),
        environment.lastTestedAt(),
        environment.deletedAt());

    updateEnvironmentRecord(syncedEnvironment);

    return snapshotRecord.toSyncStatus("SYNCED", "Metadata synced successfully.", syncedAt);
  }

  public EnvironmentSyncStatus getSyncStatus(String environmentId) {
    EnvironmentRecord environment = requireEnvironment(environmentId);
    EnvironmentSnapshotRecord snapshot = isMockMode()
        ? mockEnvironmentStore.getSnapshot(environmentId)
        : snapshotRepository.findByEnvironmentId(environmentId).orElse(null);

    if (snapshot == null) {
      return new EnvironmentSyncStatus(environmentId, environment.syncStatus(), environment.syncMessage(), environment.lastSyncedAt(), 0, 0, 0);
    }

    return snapshot.toSyncStatus(environment.syncStatus(), environment.syncMessage(), environment.lastSyncedAt());
  }

  public void softDeleteEnvironment(String environmentId) {
    EnvironmentRecord environment = requireEnvironment(environmentId);
    EnvironmentRecord deletedRecord = new EnvironmentRecord(
        environment.id(),
        environment.name(),
        environment.kind(),
        environment.region(),
        environment.clusterLabel(),
        environment.summary(),
        environment.brokerUrl(),
        environment.adminUrl(),
        environment.authMode(),
        environment.credentialReference(),
        environment.tlsEnabled(),
        environment.status(),
        environment.syncStatus(),
        environment.syncMessage(),
        environment.lastSyncedAt(),
        environment.lastTestStatus(),
        environment.lastTestMessage(),
        environment.lastTestedAt(),
        Instant.now());
    if (isMockMode()) {
      mockEnvironmentStore.softDelete(environmentId);
    } else {
      environmentRepository.update(deletedRecord);
    }
  }

  private EnvironmentRecord requireEnvironment(String environmentId) {
    if (isMockMode()) {
      return mockEnvironmentStore.findActiveById(environmentId);
    }

    return environmentRepository.findActiveById(environmentId)
        .orElseThrow(() -> new NotFoundException("Unknown environment: " + environmentId));
  }

  private boolean activeEnvironmentExists(String environmentId) {
    return isMockMode()
        ? mockEnvironmentStore.existsActiveById(environmentId)
        : environmentRepository.existsActiveById(environmentId);
  }

  private List<EnvironmentRecord> activeEnvironmentRecords() {
    return isMockMode() ? mockEnvironmentStore.findAllActive() : environmentRepository.findAllActive();
  }

  private void updateEnvironmentRecord(EnvironmentRecord record) {
    if (isMockMode()) {
      mockEnvironmentStore.update(record);
      return;
    }
    environmentRepository.update(record);
  }

  private EnvironmentSnapshotRecord storeSnapshot(String environmentId, EnvironmentSnapshot snapshot) {
    if (isMockMode()) {
      return mockEnvironmentStore.storeSnapshot(environmentId, snapshot);
    }

    snapshotRepository.upsert(environmentId, snapshot);
    return snapshotRepository.findByEnvironmentId(environmentId)
        .orElseThrow(() -> new NotFoundException("No synced metadata found for environment: " + environmentId));
  }

  private boolean isMockMode() {
    return "mock".equals(gatewayModeResolver.resolveMode());
  }

  private String blankToNull(String value) {
    return value == null || value.isBlank() ? null : value;
  }

  private void validateRequest(EnvironmentUpsertRequest request) {
    String authMode = request.authMode() == null ? "" : request.authMode().trim().toLowerCase();
    if (!SUPPORTED_AUTH_MODES.contains(authMode)) {
      throw new BadRequestException("Auth mode must be one of " + SUPPORTED_AUTH_MODES + ".");
    }

    boolean requiresCredential = !"none".equals(authMode);
    boolean hasCredential = request.credentialReference() != null && !request.credentialReference().isBlank();
    if (requiresCredential && !hasCredential) {
      throw new BadRequestException("Credential reference is required when auth mode is " + authMode + ".");
    }
  }
}
