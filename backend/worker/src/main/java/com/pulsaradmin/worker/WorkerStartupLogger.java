package com.pulsaradmin.worker;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class WorkerStartupLogger {
  private static final Logger log = LoggerFactory.getLogger(WorkerStartupLogger.class);

  @PostConstruct
  void logStartup() {
    log.info("Worker module initialized. Replay and copy jobs will be wired in a later slice.");
  }
}
