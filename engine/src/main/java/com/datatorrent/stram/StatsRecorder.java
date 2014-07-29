/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.datatorrent.stram.webapp.OperatorInfo;

/**
 * <p>StatsRecorder interface.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.3.4
 */
public interface StatsRecorder
{
  public void recordContainers(Map<String, StreamingContainerAgent> containerMap, long timestamp) throws IOException;

  public void recordOperators(List<OperatorInfo> operatorList, long timestamp) throws IOException;

}
