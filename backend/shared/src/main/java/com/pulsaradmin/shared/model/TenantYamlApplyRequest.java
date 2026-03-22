package com.pulsaradmin.shared.model;

import jakarta.validation.constraints.NotBlank;
import java.util.List;

public record TenantYamlApplyRequest(
    @NotBlank String previewId,
    @NotBlank String reason,
    List<String> confirmedChangeKeys) {
}
