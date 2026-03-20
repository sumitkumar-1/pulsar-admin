package com.pulsaradmin.api.controller;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest(properties = "app.pulsar.gateway-mode=mock")
@AutoConfigureMockMvc
class CatalogControllerTest {
  @Autowired
  private MockMvc mockMvc;

  @Test
  void shouldReturnCatalogSummary() throws Exception {
    mockMvc.perform(get("/api/v1/environments/prod/catalog"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.environmentId").value("prod"))
        .andExpect(jsonPath("$.tenants[0].name").exists())
        .andExpect(jsonPath("$.namespaces[0].tenant").exists());
  }

  @Test
  void shouldCreateTenant() throws Exception {
    mockMvc.perform(post("/api/v1/environments/prod/tenants")
            .contentType("application/json")
            .content("""
                {
                  "tenant": "finance",
                  "adminRoles": ["platform-admin"],
                  "allowedClusters": ["prod-east"]
                }
                """))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.resourceType").value("TENANT"))
        .andExpect(jsonPath("$.resourceName").value("finance"))
        .andExpect(jsonPath("$.catalogSummary.tenants[?(@.name=='finance')]").exists());
  }

  @Test
  void shouldCreateNamespace() throws Exception {
    mockMvc.perform(post("/api/v1/environments/prod/namespaces")
            .contentType("application/json")
            .content("""
                {
                  "tenant": "acme",
                  "namespace": "sandbox"
                }
                """))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.resourceType").value("NAMESPACE"))
        .andExpect(jsonPath("$.resourceName").value("acme/sandbox"))
        .andExpect(jsonPath("$.catalogSummary.namespaces[?(@.tenant=='acme' && @.namespace=='sandbox')]").exists());
  }

  @Test
  void shouldReturnNamespaceDetails() throws Exception {
    mockMvc.perform(get("/api/v1/environments/prod/namespaces/detail")
            .param("tenant", "acme")
            .param("namespace", "orders"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.tenant").value("acme"))
        .andExpect(jsonPath("$.namespace").value("orders"))
        .andExpect(jsonPath("$.policies.messageTtlInSeconds").exists());
  }

  @Test
  void shouldUpdateNamespacePolicies() throws Exception {
    mockMvc.perform(post("/api/v1/environments/prod/namespaces/policies")
            .contentType("application/json")
            .content("""
                {
                  "tenant": "acme",
                  "namespace": "orders",
                  "policies": {
                    "retentionTimeInMinutes": 2880,
                    "retentionSizeInMb": 2048,
                    "messageTtlInSeconds": 7200,
                    "deduplicationEnabled": true,
                    "backlogQuotaLimitInBytes": 1073741824,
                    "backlogQuotaLimitTimeInSeconds": 3600,
                    "dispatchRatePerTopicInMsg": 2500,
                    "dispatchRatePerTopicInByte": 1048576,
                    "publishRateInMsg": 2500,
                    "publishRateInByte": 1048576
                  },
                  "reason": "Align namespace defaults for operator testing"
                }
                """))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.tenant").value("acme"))
        .andExpect(jsonPath("$.namespaceDetails.policies.deduplicationEnabled").value(true));
  }

  @Test
  void shouldPreviewTenantYaml() throws Exception {
    mockMvc.perform(post("/api/v1/environments/prod/tenants/yaml/preview")
            .contentType("application/json")
            .content("""
                {
                  "tenant": "acme",
                  "yaml": "tenant: acme\\nnamespaces:\\n  - name: orders\\n    policies:\\n      retentionTimeInMinutes: 1440\\n    topics:\\n      - name: order-events\\n        domain: persistent\\n        partitions: 0\\n        policies:\\n          maxProducers: 10\\n"
                }
                """))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.tenant").value("acme"))
        .andExpect(jsonPath("$.previewId").exists())
        .andExpect(jsonPath("$.changes").isArray());
  }
}
