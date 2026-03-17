package com.pulsaradmin.api.controller;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
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
class TopicControllerTest {
  @Autowired
  private MockMvc mockMvc;

  @Test
  void shouldFilterTopicsBySearchAndPaginate() throws Exception {
    mockMvc.perform(get("/api/v1/environments/prod/topics")
            .param("search", "payment")
            .param("page", "0")
            .param("pageSize", "25"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.items", hasSize(1)))
        .andExpect(jsonPath("$.items[0].topic").value("payment-events"))
        .andExpect(jsonPath("$.total").value(1));
  }

  @Test
  void shouldReturnTopicDetails() throws Exception {
    mockMvc.perform(get("/api/v1/environments/prod/topics/detail")
            .param("topic", "persistent://acme/orders/payment-events"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.fullName").value("persistent://acme/orders/payment-events"))
        .andExpect(jsonPath("$.stats.backlog", greaterThan(1000)))
        .andExpect(jsonPath("$.subscriptions", hasSize(2)));
  }

  @Test
  void shouldReturnPeekMessages() throws Exception {
    mockMvc.perform(get("/api/v1/environments/prod/topics/peek")
            .param("topic", "persistent://acme/orders/payment-events")
            .param("limit", "2"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.environmentId").value("prod"))
        .andExpect(jsonPath("$.topicName").value("persistent://acme/orders/payment-events"))
        .andExpect(jsonPath("$.returnedCount").value(2))
        .andExpect(jsonPath("$.messages", hasSize(2)))
        .andExpect(jsonPath("$.messages[0].messageId").exists());
  }

  @Test
  void shouldResetCursor() throws Exception {
    mockMvc.perform(post("/api/v1/environments/prod/topics/reset-cursor")
            .contentType("application/json")
            .content("""
                {
                  "topicName": "persistent://acme/orders/payment-events",
                  "subscriptionName": "payment-settlement",
                  "target": "LATEST",
                  "timestamp": null,
                  "reason": "Clear backlog after incident validation"
                }
                """))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.environmentId").value("prod"))
        .andExpect(jsonPath("$.subscriptionName").value("payment-settlement"))
        .andExpect(jsonPath("$.target").value("LATEST"))
        .andExpect(jsonPath("$.message").exists());
  }

  @Test
  void shouldSkipMessages() throws Exception {
    mockMvc.perform(post("/api/v1/environments/prod/topics/skip-messages")
            .contentType("application/json")
            .content("""
                {
                  "topicName": "persistent://acme/orders/payment-events",
                  "subscriptionName": "payment-settlement",
                  "messageCount": 25,
                  "reason": "Skip poison messages after alert triage"
                }
                """))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.environmentId").value("prod"))
        .andExpect(jsonPath("$.subscriptionName").value("payment-settlement"))
        .andExpect(jsonPath("$.skippedMessages").value(25))
        .andExpect(jsonPath("$.message").exists());
  }

  @Test
  void shouldRejectInvalidPageSize() throws Exception {
    mockMvc.perform(get("/api/v1/environments/prod/topics").param("pageSize", "7"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.message").exists());
  }

  @Test
  void shouldReturnNotFoundForUnknownTopic() throws Exception {
    mockMvc.perform(get("/api/v1/environments/prod/topics/detail")
            .param("topic", "persistent://acme/orders/missing"))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.message").value("Unknown topic: persistent://acme/orders/missing"));
  }
}
