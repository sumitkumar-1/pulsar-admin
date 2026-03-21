package com.pulsaradmin.shared.model;

import java.util.List;

public record PlatformSummary(
    String environmentId,
    List<PlatformArtifactSummary> functions,
    List<PlatformArtifactSummary> sources,
    List<PlatformArtifactSummary> sinks,
    List<PlatformArtifactSummary> connectors) {
}
