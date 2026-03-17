package com.pulsaradmin.api.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulsaradmin.api.service.MockPulsarAdminGateway;
import com.pulsaradmin.api.service.RestPulsarAdminGateway;
import com.pulsaradmin.shared.gateway.PulsarAdminGateway;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

@Configuration
public class PulsarGatewayConfig {
  @Bean
  @ConditionalOnMissingBean(PulsarAdminGateway.class)
  public PulsarAdminGateway pulsarAdminGateway(
      ObjectMapper objectMapper,
      @Value("${app.pulsar.gateway-mode:mock}") String gatewayMode) {
    if ("rest".equalsIgnoreCase(gatewayMode)) {
      return new RestPulsarAdminGateway(RestClient.builder().build(), objectMapper);
    }

    return new MockPulsarAdminGateway();
  }
}
