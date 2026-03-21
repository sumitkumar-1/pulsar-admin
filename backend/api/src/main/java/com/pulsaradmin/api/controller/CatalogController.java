package com.pulsaradmin.api.controller;

import com.pulsaradmin.api.service.PulsarCatalogService;
import com.pulsaradmin.shared.model.CatalogMutationResponse;
import com.pulsaradmin.shared.model.CatalogSummary;
import com.pulsaradmin.shared.model.CreateNamespaceRequest;
import com.pulsaradmin.shared.model.CreateTenantRequest;
import com.pulsaradmin.shared.model.NamespaceDetails;
import com.pulsaradmin.shared.model.NamespaceDeleteRequest;
import com.pulsaradmin.shared.model.NamespaceMutationResponse;
import com.pulsaradmin.shared.model.NamespacePoliciesResponse;
import com.pulsaradmin.shared.model.NamespacePoliciesUpdateRequest;
import com.pulsaradmin.shared.model.PlatformSummary;
import com.pulsaradmin.shared.model.TenantDeleteRequest;
import com.pulsaradmin.shared.model.TenantDetails;
import com.pulsaradmin.shared.model.TenantMutationResponse;
import com.pulsaradmin.shared.model.TenantUpdateRequest;
import com.pulsaradmin.shared.model.TenantYamlApplyRequest;
import com.pulsaradmin.shared.model.TenantYamlApplyResponse;
import com.pulsaradmin.shared.model.TenantYamlPreviewRequest;
import com.pulsaradmin.shared.model.TenantYamlPreviewResponse;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
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

  @GetMapping("/tenants/detail")
  public TenantDetails getTenantDetails(
      @PathVariable("envId") String envId,
      @RequestParam("tenant") String tenant) {
    return pulsarCatalogService.getTenantDetails(envId, tenant);
  }

  @PostMapping("/tenants/update")
  public TenantMutationResponse updateTenant(
      @PathVariable("envId") String envId,
      @Valid @RequestBody TenantUpdateRequest request) {
    return pulsarCatalogService.updateTenant(envId, request);
  }

  @PostMapping("/tenants/delete")
  public TenantMutationResponse deleteTenant(
      @PathVariable("envId") String envId,
      @Valid @RequestBody TenantDeleteRequest request) {
    return pulsarCatalogService.deleteTenant(envId, request);
  }

  @PostMapping("/namespaces")
  public CatalogMutationResponse createNamespace(
      @PathVariable("envId") String envId,
      @Valid @RequestBody CreateNamespaceRequest request) {
    return pulsarCatalogService.createNamespace(envId, request);
  }

  @GetMapping("/namespaces/detail")
  public NamespaceDetails getNamespaceDetails(
      @PathVariable("envId") String envId,
      @RequestParam("tenant") String tenant,
      @RequestParam("namespace") String namespace) {
    return pulsarCatalogService.getNamespaceDetails(envId, tenant, namespace);
  }

  @PostMapping("/namespaces/policies")
  public NamespacePoliciesResponse updateNamespacePolicies(
      @PathVariable("envId") String envId,
      @Valid @RequestBody NamespacePoliciesUpdateRequest request) {
    return pulsarCatalogService.updateNamespacePolicies(envId, request);
  }

  @PostMapping("/namespaces/delete")
  public NamespaceMutationResponse deleteNamespace(
      @PathVariable("envId") String envId,
      @Valid @RequestBody NamespaceDeleteRequest request) {
    return pulsarCatalogService.deleteNamespace(envId, request);
  }

  @PostMapping("/namespaces/yaml/validate")
  public TenantYamlPreviewResponse validateYaml(
      @PathVariable("envId") String envId,
      @Valid @RequestBody TenantYamlPreviewRequest request) {
    return pulsarCatalogService.validateYamlPreview(envId, request);
  }

  @PostMapping("/namespaces/yaml/preview")
  public TenantYamlPreviewResponse previewYaml(
      @PathVariable("envId") String envId,
      @Valid @RequestBody TenantYamlPreviewRequest request) {
    return pulsarCatalogService.previewYaml(envId, request);
  }

  @PostMapping("/namespaces/yaml/apply")
  public TenantYamlApplyResponse applyYaml(
      @PathVariable("envId") String envId,
      @Valid @RequestBody TenantYamlApplyRequest request) {
    return pulsarCatalogService.applyYaml(envId, request);
  }

  @GetMapping("/platform")
  public PlatformSummary getPlatformSummary(@PathVariable("envId") String envId) {
    return pulsarCatalogService.getPlatformSummary(envId);
  }
}
