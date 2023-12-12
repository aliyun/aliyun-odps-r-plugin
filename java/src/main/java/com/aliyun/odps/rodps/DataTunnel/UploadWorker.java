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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;

/**
 * @Title: UpdateWorker.java
 * @Package com.aliyun.odps.rodps
 * @Description: TODO(添加描述)
 * @author dendi.ywd
 * @date 2015-8-7 17:59:40
 * @version V1.0
 */
public class UploadWorker implements Runnable {
  static Log LOG = LogFactory.getLog(UploadWorker.class);

  private int threadId;
  private String errorMessage;
  private boolean isSuccessful = false;
  private Context<UploadSession> context;
  private final String fileName;
  public Thread t;
  private RecordWriter writer;
  private long uploaded;
  private MiddleStorage midStorage;
  private int maxRetries = 5;

  public UploadWorker(int threadId, Context<UploadSession> context, String fileName)
      throws ROdpsException {
    this.context = context;
    this.fileName = fileName;
    this.threadId = threadId;
    this.midStorage = new SqliteMiddleStorage<UploadSession>(this.fileName, context);
    t = new Thread(this, String.valueOf(threadId));
    t.start();
  }

  public void run() {
    LOG.info("start to upload threadId=" + this.threadId);
    long blockID = (long) threadId;
    int retries = 1;
    while (retries <= maxRetries && !isSuccessful) {
      try {
        // The last opened writer would be valid only
        writer = context.getAction().openRecordWriter(blockID);
        long cnt = midStorage.writeDataTunnel(writer);
        if (this.writer != null) {
          this.writer.close(); // Occasional timeout due to server flunctation
        }
        isSuccessful = true;
        LOG.info("upload finish threadId=" + threadId + ", record=" + cnt);
      } catch (Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        this.errorMessage = sw.toString();
        if (retries <= maxRetries) {
          LOG.warn("upload failed in attempt " + retries + ", threadId=" + threadId + ", stack=" + sw.toString());
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e1) {
            LOG.error("Sleep interrupted!", e1);
          }
        } else {
          LOG.error("upload failed finally, threadId=" + threadId + ", stack=" + sw.toString());
        }
      }
      retries++;
    }
    if (this.midStorage != null) {
      this.midStorage.close();
    }
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  public boolean isSuccessful() {
    return isSuccessful;
  }

  public void setSuccessful(boolean isSuccessful) {
    this.isSuccessful = isSuccessful;
  }

  public void closeWriter() throws IOException {
    if (this.writer != null) {
      this.writer.close();
    }
  }

  public long getUploaded() {
    return this.uploaded;
  }
}