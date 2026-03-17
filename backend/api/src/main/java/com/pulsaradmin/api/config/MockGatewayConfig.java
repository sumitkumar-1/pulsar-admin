package com.pulsaradmin.api.config;

import com.pulsaradmin.api.service.MockPulsarAdminGateway;
import com.pulsaradmin.shared.gateway.PulsarAdminGateway;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MockGatewayConfig {
  @Bean
  public PulsarAdminGateway pulsarAdminGateway() {
    return new MockPulsarAdminGateway();
  }
}
