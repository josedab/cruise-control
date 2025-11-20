/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Parameters for simulation endpoint.
 *
 * <pre>
 *    POST /kafkacruisecontrol/simulate?scenario_name=[name]&amp;add_brokers=[count]
 *    &amp;remove_brokers=[id1,id2...]&amp;fail_brokers=[id1,id2...]&amp;goals=[goal1,goal2...]
 *    &amp;json=[true/false]&amp;get_response_schema=[true/false]&amp;doAs=[user]&amp;reason=[reason-for-request]
 * </pre>
 *
 * Note: This is a simplified implementation. A production version would support:
 * - JSON body for complex scenario definitions
 * - More detailed modification specifications
 * - Load multipliers and other advanced features
 */
public class SimulationParameters extends GoalBasedOptimizationParameters {
  protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;
  static {
    SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    validParameterNames.add("scenario_name");
    validParameterNames.add("add_brokers");
    validParameterNames.add("remove_brokers");
    validParameterNames.add("fail_brokers");
    validParameterNames.add("fail_rack");
    validParameterNames.addAll(GoalBasedOptimizationParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
    CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
  }

  private String _scenarioName;
  private int _addBrokers;
  private String _removeBrokers;
  private String _failBrokers;
  private String _failRack;

  public SimulationParameters() {
    super();
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _scenarioName = ParameterUtils.param(_requestContext, "scenario_name", "Unnamed Simulation");
    _addBrokers = ParameterUtils.param(_requestContext, "add_brokers", 0);
    _removeBrokers = ParameterUtils.param(_requestContext, "remove_brokers", null);
    _failBrokers = ParameterUtils.param(_requestContext, "fail_brokers", null);
    _failRack = ParameterUtils.param(_requestContext, "fail_rack", null);
  }

  public String scenarioName() {
    return _scenarioName;
  }

  public int addBrokers() {
    return _addBrokers;
  }

  public String removeBrokers() {
    return _removeBrokers;
  }

  public String failBrokers() {
    return _failBrokers;
  }

  public String failRack() {
    return _failRack;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
  }

  @Override
  public SortedSet<String> caseInsensitiveParameterNames() {
    return CASE_INSENSITIVE_PARAMETER_NAMES;
  }
}
