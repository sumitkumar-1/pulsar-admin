package com.pulsaradmin.api.controller;

import static org.hamcrest.Matchers.hasSize;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest(properties = "app.pulsar.gateway-mode=mock")
@AutoConfigureMockMvc
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class EnvironmentManagementControllerTest {
  @Autowired
  private MockMvc mockMvc;

  @Test
  void shouldCreateEnvironment() throws Exception {
    mockMvc.perform(post("/api/v1/environments")
            .contentType(MediaType.APPLICATION_JSON)
            .content("""
                {
                  "id": "local-dev",
                  "name": "Local Dev",
                  "kind": "local",
                  "region": "desktop",
                  "clusterLabel": "cluster-local",
                  "summary": "Local sandbox environment",
                  "brokerUrl": "pulsar://localhost:6650",
                  "adminUrl": "http://localhost:8080",
                  "authMode": "none",
                  "credentialReference": "",
                  "tlsEnabled": false
                }
                """))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.id").value("local-dev"))
        .andExpect(jsonPath("$.syncStatus").value("NOT_SYNCED"));
  }

  @Test
  void shouldRejectDuplicateEnvironmentId() throws Exception {
    mockMvc.perform(post("/api/v1/environments")
            .contentType(MediaType.APPLICATION_JSON)
            .content("""
                {
                  "id": "prod",
                  "name": "Duplicate Prod",
                  "kind": "prod",
                  "region": "us-east-1",
                  "clusterLabel": "cluster-prod",
                  "summary": "Duplicate",
                  "brokerUrl": "pulsar+ssl://prod-brokers:6651",
                  "adminUrl": "https://prod-admin.internal",
                  "authMode": "token",
                  "credentialReference": "vault://prod",
                  "tlsEnabled": true
                }
                """))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message").value("Environment id already exists: prod"));
  }

  @Test
  void shouldRequireCredentialsForTokenAuth() throws Exception {
    mockMvc.perform(post("/api/v1/environments")
            .contentType(MediaType.APPLICATION_JSON)
            .content("""
                {
                  "id": "prod-eu",
                  "name": "Production EU",
                  "kind": "prod",
                  "region": "eu-west-1",
                  "clusterLabel": "cluster-eu",
                  "summary": "Production token environment",
                  "brokerUrl": "pulsar+ssl://prod-eu-brokers:6651",
                  "adminUrl": "https://prod-eu-admin.internal",
                  "authMode": "token",
                  "credentialReference": "",
                  "tlsEnabled": true
                }
                """))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message").value("Credential reference is required when auth mode is token."));
  }

  @Test
  void shouldTestConnectionWithoutTriggeringSync() throws Exception {
    mockMvc.perform(post("/api/v1/environments/dev/test-connection"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.successful").value(true))
        .andExpect(jsonPath("$.syncTriggered").value(false));

    mockMvc.perform(post("/api/v1/environments/dev/sync"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.syncStatus").value("SYNCED"))
        .andExpect(jsonPath("$.topicCount").value(2));
  }

  @Test
  void shouldUpdateEnvironment() throws Exception {
    mockMvc.perform(patch("/api/v1/environments/dev")
            .contentType(MediaType.APPLICATION_JSON)
            .content("""
                {
                  "id": "dev",
                  "name": "Development Updated",
                  "kind": "dev",
                  "region": "local",
                  "clusterLabel": "cluster-dev",
                  "summary": "Updated development workflows",
                  "brokerUrl": "pulsar://dev-brokers:6650",
                  "adminUrl": "https://dev-admin.internal",
                  "authMode": "none",
                  "credentialReference": "",
                  "tlsEnabled": false
                }
                """))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.name").value("Development Updated"))
        .andExpect(jsonPath("$.lastTestStatus").value("NOT_TESTED"));
  }

  @Test
  void shouldSoftDeleteEnvironment() throws Exception {
    mockMvc.perform(delete("/api/v1/environments/qa"))
        .andExpect(status().isNoContent());

    mockMvc.perform(get("/api/v1/environments"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[?(@.id=='qa')]", hasSize(0)));
  }

  @Test
  void shouldAllowRecreatingSoftDeletedEnvironmentId() throws Exception {
    mockMvc.perform(delete("/api/v1/environments/dev"))
        .andExpect(status().isNoContent());

    mockMvc.perform(post("/api/v1/environments")
            .contentType(MediaType.APPLICATION_JSON)
            .content("""
                {
                  "id": "dev",
                  "name": "Development Recreated",
                  "kind": "dev",
                  "region": "local",
                  "clusterLabel": "cluster-dev",
                  "summary": "Recreated after cleanup",
                  "brokerUrl": "pulsar://localhost:6650",
                  "adminUrl": "http://localhost:8080",
                  "authMode": "none",
                  "credentialReference": "",
                  "tlsEnabled": false
                }
                """))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.id").value("dev"))
        .andExpect(jsonPath("$.name").value("Development Recreated"))
        .andExpect(jsonPath("$.deleted").value(false));
  }
}
