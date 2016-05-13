/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.aliyun.odps.rodps.DataTunnel;

import com.aliyun.odps.Odps;
import com.aliyun.odps.TableSchema;

/**
 * @Title: RContext.java
 * @Package com.aliyun.odps.rodps.DataTunnel
 * @Description: TODO(添加描述) 维护状态信息（连接、总行数、要读取的行数、启动的线程数）
 * @author dendi.ywd
 * @date 2015-8-7 17:52:19
 * @version V1.0
 */
public class Context<T> {

  private Odps odps;
  private final String dtEndpoint;
  private final String project;
  private final String table;
  private final String partition;
  private TableSchema schema;
  private String actionId;

  private long recordCount;

  private final long limit;
  private final String colDim;
  private final String rowDim;
  private T action;

  private int threadNumber;

  public Context(Odps odps, String dtEndpoint, String project, String table, String partition,
      long limit, String colDim, String rowDim, int threadNum) {
    this.odps = odps;
    this.dtEndpoint = dtEndpoint;
    this.project = project;
    this.table = table;
    this.partition = partition;
    this.limit = limit;
    this.colDim = colDim;
    this.rowDim = rowDim;
    this.threadNumber = threadNum;
  }

  public String getColDim() {
    return colDim;
  }

  public int getThreadNumber() {
    return threadNumber;
  }

  public void setThreadNumber(int threadNumber) {
    this.threadNumber = threadNumber;
  }

  public String getRowDim() {
    return rowDim;
  }


  public long getDownloadRecords() {
    if (limit <= 0L) {
      return this.recordCount;
    }
    if (this.recordCount > limit) {
      return limit;
    }
    return this.recordCount;
  }

  public int getActualThreads() {
    if (getDownloadRecords() < getThreadNumber() * 100) {
      return 1;
    } else {
      return this.threadNumber;
    }
  }

  public String getDtEndpoint() {
    return dtEndpoint;
  }

  public String getProject() {
    return project;
  }

  public String getTable() {
    return table;
  }

  public String getPartition() {
    return partition;
  }

  public String getActionId() {
    return actionId;
  }

  public void setActionId(String actionId) {
    this.actionId = actionId;
  }

  public long getRecordCount() {
    return recordCount;
  }

  public void setRecordCount(long recordCount) {
    this.recordCount = recordCount;
  }

  public void setAction(T action) {
    this.action = action;
  }

  public T getAction() {
    return this.action;
  }

  public Odps getOdps() {
    return this.odps;
  }

  public void setOdps(Odps odps) {
    this.odps = odps;
  }

  public TableSchema getSchema() {
    return this.schema;
  }

  public void setSchema(TableSchema schema) {
    this.schema = schema;
  }
}
