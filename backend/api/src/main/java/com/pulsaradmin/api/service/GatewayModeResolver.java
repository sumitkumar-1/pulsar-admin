package com.pulsaradmin.api.service;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

public class GatewayModeResolver {
  private final String defaultMode;

  public GatewayModeResolver(String defaultMode) {
    this.defaultMode = defaultMode == null || defaultMode.isBlank() ? "rest" : defaultMode.trim().toLowerCase();
  }

  public String resolveMode() {
    RequestAttributes attributes = RequestContextHolder.getRequestAttributes();
    if (attributes instanceof ServletRequestAttributes servletAttributes) {
      HttpServletRequest request = servletAttributes.getRequest();
      String requestedMode = request.getParameter("mode");
      if ("mock".equalsIgnoreCase(requestedMode)) {
        return "mock";
      }
      if ("rest".equalsIgnoreCase(requestedMode) || "real".equalsIgnoreCase(requestedMode)) {
        return "rest";
      }
    }

    return defaultMode;
  }
}
