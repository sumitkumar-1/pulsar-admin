package com.pulsaradmin.api.service;

import com.pulsaradmin.shared.gateway.PulsarAdminGateway;
import com.pulsaradmin.shared.model.CreateNamespaceRequest;
import com.pulsaradmin.shared.model.CreateSubscriptionRequest;
import com.pulsaradmin.shared.model.CreateTenantRequest;
import com.pulsaradmin.shared.model.CreateTopicRequest;
import com.pulsaradmin.shared.model.ConsumeMessagesRequest;
import com.pulsaradmin.shared.model.ConsumeMessagesResponse;
import com.pulsaradmin.shared.model.EnvironmentConnectionTestResult;
import com.pulsaradmin.shared.model.EnvironmentDetails;
import com.pulsaradmin.shared.model.EnvironmentSnapshot;
import com.pulsaradmin.shared.model.NamespaceDetails;
import com.pulsaradmin.shared.model.NamespacePolicies;
import com.pulsaradmin.shared.model.PeekMessagesResponse;
import com.pulsaradmin.shared.model.PlatformSummary;
import com.pulsaradmin.shared.model.PublishMessageRequest;
import com.pulsaradmin.shared.model.PublishMessageResponse;
import com.pulsaradmin.shared.model.ResetCursorRequest;
import com.pulsaradmin.shared.model.ResetCursorResponse;
import com.pulsaradmin.shared.model.SkipMessagesRequest;
import com.pulsaradmin.shared.model.SkipMessagesResponse;
import com.pulsaradmin.shared.model.TenantDetails;
import com.pulsaradmin.shared.model.TenantUpdateRequest;
import com.pulsaradmin.shared.model.TerminateTopicRequest;
import com.pulsaradmin.shared.model.TerminateTopicResponse;
import com.pulsaradmin.shared.model.TopicPolicies;
import com.pulsaradmin.shared.model.UnloadTopicRequest;
import com.pulsaradmin.shared.model.UnloadTopicResponse;
import java.util.List;

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
  public void createTenant(EnvironmentDetails environment, CreateTenantRequest request) {
    activeGateway().createTenant(environment, request);
  }

  @Override
  public void createNamespace(EnvironmentDetails environment, CreateNamespaceRequest request) {
    activeGateway().createNamespace(environment, request);
  }

  @Override
  public TenantDetails getTenantDetails(EnvironmentDetails environment, String tenant) {
    return activeGateway().getTenantDetails(environment, tenant);
  }

  @Override
  public TenantDetails updateTenant(EnvironmentDetails environment, TenantUpdateRequest request) {
    return activeGateway().updateTenant(environment, request);
  }

  @Override
  public void deleteTenant(EnvironmentDetails environment, String tenant) {
    activeGateway().deleteTenant(environment, tenant);
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
  public TerminateTopicResponse terminateTopic(EnvironmentDetails environment, TerminateTopicRequest request) {
    return activeGateway().terminateTopic(environment, request);
  }

  @Override
  public TopicPolicies getTopicPolicies(EnvironmentDetails environment, String topicName) {
    return activeGateway().getTopicPolicies(environment, topicName);
  }

  @Override
  public TopicPolicies updateTopicPolicies(EnvironmentDetails environment, String topicName, TopicPolicies policies) {
    return activeGateway().updateTopicPolicies(environment, topicName, policies);
  }

  @Override
  public NamespaceDetails getNamespaceDetails(EnvironmentDetails environment, String tenant, String namespace) {
    return activeGateway().getNamespaceDetails(environment, tenant, namespace);
  }

  @Override
  public NamespacePolicies updateNamespacePolicies(
      EnvironmentDetails environment,
      String tenant,
      String namespace,
      NamespacePolicies policies) {
    return activeGateway().updateNamespacePolicies(environment, tenant, namespace, policies);
  }

  @Override
  public PublishMessageResponse publishMessage(EnvironmentDetails environment, PublishMessageRequest request) {
    return activeGateway().publishMessage(environment, request);
  }

  @Override
  public ConsumeMessagesResponse consumeMessages(EnvironmentDetails environment, ConsumeMessagesRequest request) {
    return activeGateway().consumeMessages(environment, request);
  }

  @Override
  public void deleteTopic(EnvironmentDetails environment, String topicName) {
    activeGateway().deleteTopic(environment, topicName);
  }

  @Override
  public void deleteNamespace(EnvironmentDetails environment, String tenant, String namespace) {
    activeGateway().deleteNamespace(environment, tenant, namespace);
  }

  @Override
  public PlatformSummary getPlatformSummary(EnvironmentDetails environment, List<String> namespaces) {
    return activeGateway().getPlatformSummary(environment, namespaces);
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
