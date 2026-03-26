package com.pulsaradmin.api.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulsaradmin.api.service.GatewayModeResolver;
import com.pulsaradmin.api.service.MockPulsarAdminGateway;
import com.pulsaradmin.api.service.RestPulsarAdminGateway;
import com.pulsaradmin.api.service.RoutingPulsarAdminGateway;
import com.pulsaradmin.shared.gateway.PulsarAdminGateway;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.client.RestClient;

@Configuration
public class PulsarGatewayConfig {
  @Bean
  public PulsarAdminGateway mockPulsarAdminGateway() {
    return new MockPulsarAdminGateway();
  }

  @Bean
  public PulsarAdminGateway restPulsarAdminGateway(
      ObjectMapper objectMapper,
      @Value("${app.pulsar.admin-rest-connect-timeout-ms:3000}") int connectTimeoutMs,
      @Value("${app.pulsar.admin-rest-read-timeout-ms:5000}") int readTimeoutMs,
      @Value("${app.pulsar.inventory-sync-concurrency:8}") int inventorySyncConcurrency) {
    SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
    requestFactory.setConnectTimeout(connectTimeoutMs);
    requestFactory.setReadTimeout(readTimeoutMs);

    return new RestPulsarAdminGateway(
        RestClient.builder().requestFactory(requestFactory).build(),
        objectMapper,
        RestPulsarAdminGateway::buildClient,
        inventorySyncConcurrency);
  }

  @Bean
  public GatewayModeResolver gatewayModeResolver(
      @Value("${app.pulsar.gateway-mode:mock}") String gatewayMode) {
    return new GatewayModeResolver(gatewayMode);
  }

  @Bean
  @Primary
  public PulsarAdminGateway pulsarAdminGateway(
      GatewayModeResolver gatewayModeResolver,
      @Qualifier("mockPulsarAdminGateway") PulsarAdminGateway mockPulsarAdminGateway,
      @Qualifier("restPulsarAdminGateway") PulsarAdminGateway restPulsarAdminGateway) {
    return new RoutingPulsarAdminGateway(
        mockPulsarAdminGateway,
        restPulsarAdminGateway,
        gatewayModeResolver);
  }
}
