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

@SpringBootTest
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
}
