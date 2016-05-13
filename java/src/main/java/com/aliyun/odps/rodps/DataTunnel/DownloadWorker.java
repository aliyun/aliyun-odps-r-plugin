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
    LOG.info(threadId + ":" + String.valueOf(startRecordNumber) + " "
        + String.valueOf(downloadRecordNumber));
    t = new Thread(this, String.valueOf(threadId));
    t.start();
  }

  public void run() {
    try {
      RecordReader reader = null;
      if (downloadRecordNumber > 0) {
        reader = context.getAction().openRecordReader(startRecordNumber, downloadRecordNumber);
      }
      midStorage.saveDtData(reader, downloadRecordNumber);

      LOG.info("threadId=" + this.threadId + " download finished,loadedRecordNum="
          + this.loadedRecordNum);
      isSuccessful = true;
    } catch (Exception e) {
      LOG.error("DownloadWorker fail:", e);
      this.errorMessage = e.getMessage();
    } finally {
      midStorage.close();
    }
  }

  public boolean IsSuccessful() {
    return isSuccessful;
  }

  public String getErrorMessage() {
    return errorMessage;
  }
}
