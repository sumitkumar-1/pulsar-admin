package com.pulsaradmin.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "com.pulsaradmin")
@EnableScheduling
public class PulsarAdminWorkerApplication {
  public static void main(String[] args) {
    SpringApplication.run(PulsarAdminWorkerApplication.class, args);
  }
}
