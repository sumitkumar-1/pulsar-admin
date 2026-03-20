package com.pulsaradmin.api.service;

import java.util.List;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class EnvironmentSeeder implements CommandLineRunner {
  private final EnvironmentManagementService environmentManagementService;

  public EnvironmentSeeder(
      EnvironmentManagementService environmentManagementService) {
    this.environmentManagementService = environmentManagementService;
  }

  @Override
  public void run(String... args) {
    List<String> activeEnvironmentIds = environmentManagementService.getEnvironments().stream()
        .map(environment -> environment.id())
        .toList();
    activeEnvironmentIds.forEach(environmentId -> {
      try {
        environmentManagementService.testConnection(environmentId);
      } catch (RuntimeException ignored) {
        // Keep local startup resilient when an existing environment cannot be synced automatically.
      }
    });
  }
}
