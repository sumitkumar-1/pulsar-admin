package com.pulsaradmin.api.controller;

import com.pulsaradmin.api.service.EnvironmentManagementService;
import com.pulsaradmin.shared.model.EnvironmentConnectionTestResult;
import com.pulsaradmin.shared.model.EnvironmentDetails;
import com.pulsaradmin.shared.model.EnvironmentSummary;
import com.pulsaradmin.shared.model.EnvironmentSyncStatus;
import com.pulsaradmin.shared.model.EnvironmentUpsertRequest;
import jakarta.validation.Valid;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/environments")
public class EnvironmentManagementController {
  private final EnvironmentManagementService environmentManagementService;

  public EnvironmentManagementController(EnvironmentManagementService environmentManagementService) {
    this.environmentManagementService = environmentManagementService;
  }

  @GetMapping
  public List<EnvironmentSummary> getEnvironments() {
    return environmentManagementService.getEnvironments();
  }

  @GetMapping("/{envId}")
  public EnvironmentDetails getEnvironment(@PathVariable("envId") String envId) {
    return environmentManagementService.getEnvironment(envId);
  }

  @PostMapping
  @ResponseStatus(HttpStatus.CREATED)
  public EnvironmentDetails createEnvironment(@Valid @RequestBody EnvironmentUpsertRequest request) {
    return environmentManagementService.createEnvironment(request);
  }

  @PatchMapping("/{envId}")
  public EnvironmentDetails updateEnvironment(
      @PathVariable("envId") String envId,
      @Valid @RequestBody EnvironmentUpsertRequest request) {
    return environmentManagementService.updateEnvironment(envId, request);
  }

  @PostMapping("/{envId}/test-connection")
  public EnvironmentConnectionTestResult testConnection(@PathVariable("envId") String envId) {
    return environmentManagementService.testConnection(envId);
  }

  @PostMapping("/{envId}/sync")
  public EnvironmentSyncStatus syncEnvironment(@PathVariable("envId") String envId) {
    return environmentManagementService.syncEnvironment(envId);
  }

  @GetMapping("/{envId}/sync-status")
  public EnvironmentSyncStatus getSyncStatus(@PathVariable("envId") String envId) {
    return environmentManagementService.getSyncStatus(envId);
  }

  @DeleteMapping("/{envId}")
  @ResponseStatus(HttpStatus.NO_CONTENT)
  public void softDelete(@PathVariable("envId") String envId) {
    environmentManagementService.softDeleteEnvironment(envId);
  }
}
