package com.pulsaradmin.api.service;

import com.pulsaradmin.api.support.BadRequestException;
import com.pulsaradmin.shared.model.EnvironmentDetails;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

final class EnvironmentCredentials {
  private EnvironmentCredentials() {
  }

  static AuthHeaders resolve(EnvironmentDetails environment) {
    String authMode = normalize(environment.authMode());
    String credentialReference = environment.credentialReference();

    return switch (authMode) {
      case "", "none" -> AuthHeaders.none();
      case "token" -> AuthHeaders.bearer(resolveReference(credentialReference, "token"));
      case "basic" -> AuthHeaders.basic(resolveReference(credentialReference, "basic"));
      case "mtls" -> throw new BadRequestException(
          "mTLS environments are not yet supported by the live gateway. Use mock mode for now.");
      default -> throw new BadRequestException("Unsupported auth mode: " + environment.authMode());
    };
  }

  private static String resolveReference(String credentialReference, String authMode) {
    if (credentialReference == null || credentialReference.isBlank()) {
      throw new BadRequestException("Credential reference is required when auth mode is " + authMode + ".");
    }

    String trimmed = credentialReference.trim();
    if (trimmed.startsWith("env://")) {
      String variableName = trimmed.substring("env://".length()).trim();
      if (variableName.isBlank()) {
        throw new BadRequestException("Credential env reference must include a variable name.");
      }

      String value = System.getenv(variableName);
      if (value == null || value.isBlank()) {
        throw new BadRequestException("Environment variable " + variableName + " is not set.");
      }
      return value.trim();
    }

    if (trimmed.startsWith("vault://")) {
      throw new BadRequestException(
          "Vault-style credential references are not wired into this prototype yet. Use a raw value or env://VAR_NAME.");
    }

    if (trimmed.startsWith("inline://")) {
      String value = trimmed.substring("inline://".length()).trim();
      if (value.isBlank()) {
        throw new BadRequestException("Inline credentials cannot be empty.");
      }
      return value;
    }

    return trimmed;
  }

  private static String normalize(String value) {
    return value == null ? "" : value.trim().toLowerCase();
  }

  record AuthHeaders(String authorizationHeader) {
    static AuthHeaders none() {
      return new AuthHeaders(null);
    }

    static AuthHeaders bearer(String token) {
      String value = token;
      if (token.regionMatches(true, 0, "Bearer ", 0, "Bearer ".length())) {
        value = token.substring("Bearer ".length()).trim();
      } else if (token.regionMatches(true, 0, "token:", 0, "token:".length())) {
        value = token.substring("token:".length()).trim();
      }

      if (value.isBlank()) {
        throw new BadRequestException("Token credentials cannot be empty.");
      }

      return new AuthHeaders("Bearer " + value);
    }

    static AuthHeaders basic(String credentials) {
      if (!credentials.contains(":")) {
        throw new BadRequestException("Basic credentials must use username:password format.");
      }

      String encoded = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
      return new AuthHeaders("Basic " + encoded);
    }

    boolean present() {
      return authorizationHeader != null && !authorizationHeader.isBlank();
    }
  }
}
