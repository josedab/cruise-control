/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.validation;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command-line tool for validating Cruise Control configuration files.
 *
 * Usage:
 *   java -cp cruise-control.jar com.linkedin.kafka.cruisecontrol.config.validation.ConfigValidatorTool <config-file>
 *
 * Exit codes:
 *   0 - Configuration is valid
 *   1 - Configuration has errors
 *   2 - Configuration has warnings only
 *   3 - Tool error (invalid arguments, file not found, etc.)
 */
public class ConfigValidatorTool {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigValidatorTool.class);

  public static void main(String[] args) {
    if (args.length != 1) {
      System.err.println("Usage: ConfigValidatorTool <config-file>");
      System.err.println();
      System.err.println("Validates Cruise Control configuration without starting the service.");
      System.err.println();
      System.err.println("Exit codes:");
      System.err.println("  0 - Configuration is valid");
      System.err.println("  1 - Configuration has errors");
      System.err.println("  2 - Configuration has warnings only");
      System.err.println("  3 - Tool error (invalid arguments, file not found, etc.)");
      System.exit(3);
    }

    String configFile = args[0];
    System.out.println("Validating configuration: " + configFile);
    System.out.println();

    try {
      // Load configuration
      Properties props = new Properties();
      try (FileInputStream fis = new FileInputStream(configFile)) {
        props.load(fis);
      } catch (IOException e) {
        System.err.println("Error loading configuration file: " + e.getMessage());
        System.exit(3);
      }

      // Create config object
      KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props, false);

      // Run validation
      ConfigurationValidator validator = new ConfigurationValidator(config);
      ValidationResult result = validator.validate();

      // Format and display report
      ValidationReportFormatter formatter = new ValidationReportFormatter();
      String report = formatter.format(result);
      System.out.println(report);

      // Exit with appropriate code
      if (result.hasErrors()) {
        System.exit(1);
      } else if (result.hasWarnings()) {
        System.exit(2);
      } else {
        System.exit(0);
      }
    } catch (Exception e) {
      System.err.println("Error validating configuration: " + e.getMessage());
      LOG.error("Validation error", e);
      System.exit(3);
    }
  }
}
