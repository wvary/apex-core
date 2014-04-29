/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.api;

import org.apache.hadoop.conf.Configuration;

/**
 * Interface to be implemented for Java based streaming application declaration.
 * <p>
 * An application is the top level DAG with external configuration for
 * application master / engine settings, application specific properties or
 * overrides for individual operators in the DAG. <br>
 * Application launchers (CLI) use the interface to identify application DAGs
 * within jar files and supply the configuration upon instantiation.
 * <p>
 * Operator properties in the DAG can be configured externally. When an
 * application is launched from the CLI, any settings in dt-site.xml would
 * override property values in the DAG. It is therefore possible to have
 * defaults in the DAG code and supply environment/launch context specific
 * settings through the configuration.
 *
 * @since 0.3.2
 */
public interface StreamingApplication
{
  String DT_PREFIX = "dt.";
  /**
   * Constant
   * <code>LAUNCHMODE_YARN="yarn"</code>
   */
  String LAUNCHMODE_YARN = "yarn";
  /**
   * Constant
   * <code>LAUNCHMODE_LOCAL="local"</code>
   */
  String LAUNCHMODE_LOCAL = "local";
  /**
   * Launch mode for the application.
   * Used in the client to set configuration depending on how the DAG is executed.
   */
  String LAUNCH_MODE = DT_PREFIX + "launchmode";

  /**
   * <p>populateDAG.</p>
   */
  void populateDAG(DAG dag, Configuration conf);

}
