package com.pulsaradmin.api.controller;

import com.pulsaradmin.api.service.PulsarCatalogService;
import com.pulsaradmin.shared.model.EnvironmentHealth;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/environments")
public class EnvironmentController {
  private final PulsarCatalogService pulsarCatalogService;

  public EnvironmentController(PulsarCatalogService pulsarCatalogService) {
    this.pulsarCatalogService = pulsarCatalogService;
  }

  @GetMapping("/{envId}/health")
  public EnvironmentHealth getEnvironmentHealth(@PathVariable("envId") String envId) {
    return pulsarCatalogService.getEnvironmentHealth(envId);
  }
}
