package com.pulsaradmin.shared.model;

import jakarta.validation.constraints.NotBlank;

public record CreateNamespaceRequest(
    @NotBlank String tenant,
    @NotBlank String namespace) {
}
