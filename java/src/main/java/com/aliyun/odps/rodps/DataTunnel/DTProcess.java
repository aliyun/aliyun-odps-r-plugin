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

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @Title: DTProcess.java
 * @Package com.aliyun.odps.rodps.DataTunnel
 * @Description: TODO(添加描述)
 * @author dendi.ywd
 * @date 2015-8-7 17:54:59
 * @version V1.0
 */
public abstract class DTProcess<T, C> {
  static Log LOG = LogFactory.getLog(DTProcess.class.getSuperclass());
  protected Context<C> context;

  public DTProcess(Context<C> context) {
    this.context = context;
  }

  public List<T> createWorkerList(String fileName) throws ROdpsException {
    int threadNum = context.getActualThreads();

    LOG.info(String.format("start to create %d processing workers", threadNum));

    long recordNumPerThread = this.context.getDownloadRecords() / threadNum;
    LOG.info("record number per thread:" + String.valueOf(recordNumPerThread));
    List<T> workers = new ArrayList<T>();
    for (int i = 0; i < threadNum; ++i) {
      T worker;
      Long records =
          (i == threadNum - 1 ? (this.context.getDownloadRecords() - i * recordNumPerThread)
              : recordNumPerThread);
      worker =
          createWorker(i, context, i * recordNumPerThread, records, createTempFileName(fileName, i));
      workers.add(worker);
    }
    LOG.info("finish creating processing workers");
    return workers;
  }

  public abstract T createWorker(int threadId, Context<C> context, long startRecordNumber,
      long downloadRecordNumber, String fileName) throws ROdpsException;

  protected static String createTempFileName(String fileName, int index) {
    return fileName + "_" + index;
  }

}
