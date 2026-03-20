package com.pulsaradmin.api.controller;

import com.pulsaradmin.api.service.PulsarCatalogService;
import com.pulsaradmin.shared.model.CatalogMutationResponse;
import com.pulsaradmin.shared.model.CatalogSummary;
import com.pulsaradmin.shared.model.CreateNamespaceRequest;
import com.pulsaradmin.shared.model.CreateTenantRequest;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/environments/{envId}")
public class CatalogController {
  private final PulsarCatalogService pulsarCatalogService;

  public CatalogController(PulsarCatalogService pulsarCatalogService) {
    this.pulsarCatalogService = pulsarCatalogService;
  }

  @GetMapping("/catalog")
  public CatalogSummary getCatalogSummary(@PathVariable("envId") String envId) {
    return pulsarCatalogService.getCatalogSummary(envId);
  }

  @PostMapping("/tenants")
  public CatalogMutationResponse createTenant(
      @PathVariable("envId") String envId,
      @Valid @RequestBody CreateTenantRequest request) {
    return pulsarCatalogService.createTenant(envId, request);
  }

  @PostMapping("/namespaces")
  public CatalogMutationResponse createNamespace(
      @PathVariable("envId") String envId,
      @Valid @RequestBody CreateNamespaceRequest request) {
    return pulsarCatalogService.createNamespace(envId, request);
  }
}
