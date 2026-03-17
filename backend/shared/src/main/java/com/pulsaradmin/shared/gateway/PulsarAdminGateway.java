package com.pulsaradmin.shared.gateway;

import com.pulsaradmin.shared.model.EnvironmentConnectionTestResult;
import com.pulsaradmin.shared.model.EnvironmentDetails;
import com.pulsaradmin.shared.model.EnvironmentSnapshot;

public interface PulsarAdminGateway {
  EnvironmentConnectionTestResult testConnection(EnvironmentDetails environment);

  EnvironmentSnapshot syncMetadata(EnvironmentDetails environment);
}
