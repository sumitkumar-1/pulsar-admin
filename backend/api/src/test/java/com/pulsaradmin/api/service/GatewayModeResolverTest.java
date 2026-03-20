package com.pulsaradmin.api.service;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

class GatewayModeResolverTest {
  @AfterEach
  void clearRequestAttributes() {
    RequestContextHolder.resetRequestAttributes();
  }

  @Test
  void shouldUseMockWhenModeQueryParameterRequestsIt() {
    MockHttpServletRequest request = new MockHttpServletRequest();
    request.setParameter("mode", "mock");
    RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));

    GatewayModeResolver resolver = new GatewayModeResolver("rest");

    assertThat(resolver.resolveMode()).isEqualTo("mock");
  }

  @Test
  void shouldFallbackToConfiguredDefaultWithoutQueryOverride() {
    GatewayModeResolver resolver = new GatewayModeResolver("rest");

    assertThat(resolver.resolveMode()).isEqualTo("rest");
  }
}
