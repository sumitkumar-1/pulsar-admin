package com.pulsaradmin.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.pulsaradmin")
public class PulsarAdminWorkerApplication {
  public static void main(String[] args) {
    SpringApplication.run(PulsarAdminWorkerApplication.class, args);
  }
}
