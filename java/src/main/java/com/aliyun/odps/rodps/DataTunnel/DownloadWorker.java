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

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;

/**
 * @Title: DownloadWorker.java
 * @Package com.aliyun.odps.rodps.DataTunnel
 * @Description: TODO(添加描述)
 * @author dendi.ywd
 * @date 2015-8-7 17:53:55
 * @version V1.0
 */
public class DownloadWorker implements Runnable {
  static Log LOG = LogFactory.getLog(DownloadWorker.class);
  private final long startRecordNumber;
  private final long downloadRecordNumber;
  private String errorMessage;
  private boolean isSuccessful;
  private final String savePath;
  private long loadedRecordNum;
  private final Context<DownloadSession> context;
  public Thread t;
  private int threadId;
  private MiddleStorage midStorage;
  private int maxRetries = 5;

  DownloadWorker(int threadId, Context<DownloadSession> context, long startRecordNumber,
      long downloadRecordNumber, String savePath) throws ROdpsException {
    this.threadId = threadId;
    this.startRecordNumber = startRecordNumber;
    this.downloadRecordNumber = downloadRecordNumber;
    this.loadedRecordNum = 0;
    this.isSuccessful = false;
    this.savePath = savePath;
    this.context = context;
    this.midStorage = new SqliteMiddleStorage<DownloadSession>(this.savePath, context);
    LOG.debug(threadId + ":" + String.valueOf(startRecordNumber) + " "
        + String.valueOf(downloadRecordNumber));
    t = new Thread(this, String.valueOf(threadId));
    t.start();
  }

  public void run() {
    LOG.info("start to download threadId=" + this.threadId);
    int retries = 1;
    while (retries <= maxRetries && !isSuccessful) {
      try {
        RecordReader reader = null;
        if (downloadRecordNumber > 0) {
          reader = context.getAction().openRecordReader(startRecordNumber, downloadRecordNumber);
        }
        loadedRecordNum = midStorage.readDataTunnel(reader, downloadRecordNumber);
        LOG.info("threadId=" + this.threadId + " download finished, record="
            + this.loadedRecordNum);
        isSuccessful = true;
      } catch (Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        this.errorMessage = sw.toString();
        if (retries <= maxRetries) {
          LOG.warn("download failed in attempt " + retries + ", threadId=" + threadId + ", stack=" + sw.toString());
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e1) {
            LOG.error("Sleep interrupted!", e1);
          }
        } else {
          LOG.error("download failed, threadId=" + threadId + ", stack=" + sw.toString());
        }
      }
      retries++;
    }
    if (this.midStorage != null) {
      this.midStorage.close();
    }
  }

  public boolean IsSuccessful() {
    return isSuccessful;
  }

  public String getErrorMessage() {
    return errorMessage;
  }
}