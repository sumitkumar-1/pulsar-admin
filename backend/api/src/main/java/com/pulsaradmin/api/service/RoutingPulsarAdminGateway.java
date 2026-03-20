package com.pulsaradmin.api.service;

import com.pulsaradmin.shared.gateway.PulsarAdminGateway;
import com.pulsaradmin.shared.model.CreateSubscriptionRequest;
import com.pulsaradmin.shared.model.CreateTopicRequest;
import com.pulsaradmin.shared.model.EnvironmentConnectionTestResult;
import com.pulsaradmin.shared.model.EnvironmentDetails;
import com.pulsaradmin.shared.model.EnvironmentSnapshot;
import com.pulsaradmin.shared.model.PeekMessagesResponse;
import com.pulsaradmin.shared.model.ResetCursorRequest;
import com.pulsaradmin.shared.model.ResetCursorResponse;
import com.pulsaradmin.shared.model.SkipMessagesRequest;
import com.pulsaradmin.shared.model.SkipMessagesResponse;
import com.pulsaradmin.shared.model.UnloadTopicRequest;
import com.pulsaradmin.shared.model.UnloadTopicResponse;

public class RoutingPulsarAdminGateway implements PulsarAdminGateway {
  private final PulsarAdminGateway mockGateway;
  private final PulsarAdminGateway restGateway;
  private final GatewayModeResolver gatewayModeResolver;

  public RoutingPulsarAdminGateway(
      PulsarAdminGateway mockGateway,
      PulsarAdminGateway restGateway,
      GatewayModeResolver gatewayModeResolver) {
    this.mockGateway = mockGateway;
    this.restGateway = restGateway;
    this.gatewayModeResolver = gatewayModeResolver;
  }

  @Override
  public EnvironmentConnectionTestResult testConnection(EnvironmentDetails environment) {
    return activeGateway().testConnection(environment);
  }

  @Override
  public EnvironmentSnapshot syncMetadata(EnvironmentDetails environment) {
    return activeGateway().syncMetadata(environment);
  }

  @Override
  public void createTopic(EnvironmentDetails environment, CreateTopicRequest request) {
    activeGateway().createTopic(environment, request);
  }

  @Override
  public void createSubscription(EnvironmentDetails environment, CreateSubscriptionRequest request) {
    activeGateway().createSubscription(environment, request);
  }

  @Override
  public void deleteSubscription(EnvironmentDetails environment, String topicName, String subscriptionName) {
    activeGateway().deleteSubscription(environment, topicName, subscriptionName);
  }

  @Override
  public PeekMessagesResponse peekMessages(EnvironmentDetails environment, String topicName, int limit) {
    return activeGateway().peekMessages(environment, topicName, limit);
  }

  @Override
  public ResetCursorResponse resetCursor(EnvironmentDetails environment, ResetCursorRequest request) {
    return activeGateway().resetCursor(environment, request);
  }

  @Override
  public SkipMessagesResponse skipMessages(EnvironmentDetails environment, SkipMessagesRequest request) {
    return activeGateway().skipMessages(environment, request);
  }

  @Override
  public UnloadTopicResponse unloadTopic(EnvironmentDetails environment, UnloadTopicRequest request) {
    return activeGateway().unloadTopic(environment, request);
  }

  private PulsarAdminGateway activeGateway() {
    return "mock".equals(gatewayModeResolver.resolveMode()) ? mockGateway : restGateway;
  }
}
