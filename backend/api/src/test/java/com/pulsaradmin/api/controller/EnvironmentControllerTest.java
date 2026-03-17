package com.pulsaradmin.api.controller;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
class EnvironmentControllerTest {
  @Autowired
  private MockMvc mockMvc;

  @Test
  void shouldReturnEnvironmentSummaries() throws Exception {
    mockMvc.perform(get("/api/v1/environments"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[?(@.id=='prod')]", hasSize(1)))
        .andExpect(jsonPath("$[?(@.id=='stable')]", hasSize(1)));
  }

  @Test
  void shouldReturnEnvironmentHealth() throws Exception {
    mockMvc.perform(get("/api/v1/environments/prod/health"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.environmentId").value("prod"))
        .andExpect(jsonPath("$.message", containsString("successfully")));
  }

  @Test
  void shouldReturnNotFoundForUnknownEnvironmentHealth() throws Exception {
    mockMvc.perform(get("/api/v1/environments/nope/health"))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.message").value("Unknown environment: nope"));
  }
}
