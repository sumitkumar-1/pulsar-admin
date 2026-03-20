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
import org.springframework.web.client.RestClient;

@Configuration
public class PulsarGatewayConfig {
  @Bean(autowireCandidate = false)
  public PulsarAdminGateway mockPulsarAdminGateway() {
    return new MockPulsarAdminGateway();
  }

  @Bean(autowireCandidate = false)
  public PulsarAdminGateway restPulsarAdminGateway(ObjectMapper objectMapper) {
    return new RestPulsarAdminGateway(RestClient.builder().build(), objectMapper);
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
      ObjectMapper objectMapper) {
    return new RoutingPulsarAdminGateway(
        mockPulsarAdminGateway(),
        restPulsarAdminGateway(objectMapper),
        gatewayModeResolver);
  }
}
