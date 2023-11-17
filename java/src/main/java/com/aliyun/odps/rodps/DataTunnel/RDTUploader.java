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
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;


public class RDTUploader extends DTProcess<UploadWorker, UploadSession> {
  static Log LOG = LogFactory.getLog(RDTDownloader.class);


  public RDTUploader(Context<UploadSession> context) throws IOException, ROdpsException {
    super(context);
  }

  public void upload(String dataFilePath) throws ROdpsException, IOException {
    TableTunnel tunnel = new TableTunnel(context.getOdps());
    if (context.getDtEndpoint() != null) {
      tunnel.setEndpoint(context.getDtEndpoint());
    }
    UploadSession uploadSession;
    try {
      if (context.getPartition() != null && !context.getPartition().isEmpty()) {
        PartitionSpec partitionSpec = new PartitionSpec(context.getPartition());
        uploadSession =
            tunnel.createUploadSession(context.getProject(), context.getTable(), partitionSpec);
      } else {
        uploadSession = tunnel.createUploadSession(context.getProject(), context.getTable());
      }
      context.setAction(uploadSession);
      context.setSchema(context.getAction().getSchema());
    } catch (TunnelException e) {
      throw new ROdpsException(e.getErrorCode() + e.getErrorMsg());
    }

    LOG.info("upload session ID: " + uploadSession.getId());

    List<UploadWorker> workers = this.createWorkerList(dataFilePath);
    try {
      String errorMessage = "";
      LOG.debug("wait for upload end");
      for (UploadWorker worker : workers) {
        worker.t.join();
        if (!worker.isSuccessful()) {
          LOG.error("thread fail met!");
          errorMessage += worker.getErrorMessage();
        }
      }
      Long[] blockList = new Long[workers.size()];
      for (int i = 0; i < workers.size(); i++)
        blockList[i] = Long.valueOf(i);
      uploadSession.commit(blockList);
      if (!errorMessage.isEmpty()) {
        throw new IOException(errorMessage);
      }
      LOG.info("commit success");
    } catch (InterruptedException e) {
      throw new ROdpsException(e);

    } catch (TunnelException e) {
      throw new ROdpsException(e);
    }
  }

  @Override
  public UploadWorker createWorker(int threadId, Context<UploadSession> context,
      long startRecordNumber, long downloadRecordNumber, String fileName) throws ROdpsException {
    return new UploadWorker(threadId, context, fileName);
  }
}
