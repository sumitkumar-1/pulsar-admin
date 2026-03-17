package com.pulsaradmin.api.service;

import com.pulsaradmin.shared.model.EnvironmentUpsertRequest;
import java.util.List;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class EnvironmentSeeder implements CommandLineRunner {
  private final EnvironmentRepository environmentRepository;
  private final EnvironmentManagementService environmentManagementService;

  public EnvironmentSeeder(
      EnvironmentRepository environmentRepository,
      EnvironmentManagementService environmentManagementService) {
    this.environmentRepository = environmentRepository;
    this.environmentManagementService = environmentManagementService;
  }

  @Override
  public void run(String... args) {
    if (!environmentRepository.findAllActive().isEmpty()) {
      return;
    }

    List<EnvironmentUpsertRequest> defaults = List.of(
        new EnvironmentUpsertRequest("prod", "Production", "prod", "us-east-1", "cluster-a", "Primary customer traffic", "pulsar+ssl://prod-brokers:6651", "https://prod-admin.internal", "token", "vault://pulsar/prod", true),
        new EnvironmentUpsertRequest("stable", "Stable", "stable", "us-east-1", "cluster-b", "Pre-release validation", "pulsar+ssl://stable-brokers:6651", "https://stable-admin.internal", "token", "vault://pulsar/stable", true),
        new EnvironmentUpsertRequest("qa", "QA", "qa", "us-east-2", "cluster-c", "Regression and release testing", "pulsar://qa-brokers:6650", "https://qa-admin.internal", "basic", "vault://pulsar/qa", false),
        new EnvironmentUpsertRequest("dev", "Development", "dev", "local", "cluster-dev", "Everyday engineering workflows", "pulsar://dev-brokers:6650", "https://dev-admin.internal", "none", "", false));

    defaults.forEach(request -> {
      environmentManagementService.createEnvironment(request);
      environmentManagementService.testConnection(request.id());
    });
  }
}
