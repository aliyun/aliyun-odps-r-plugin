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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;

public class RDTDownloader extends DTProcess<DownloadWorker, DownloadSession> {
  static Log LOG = LogFactory.getLog(RDTDownloader.class);

  public RDTDownloader(Context<DownloadSession> context) throws IOException {
    super(context);
  }

  public List<List<String>> downloadTable(String tempDataFile) throws ROdpsException, IOException {
    if (null == tempDataFile) {
      throw new ROdpsException("Internal Error: temp data file is null");
    }
    TableTunnel tunnel = new TableTunnel(context.getOdps());
    if (context.getDtEndpoint() != null) {
      tunnel.setEndpoint(context.getDtEndpoint());
    }
    LOG.info("start to download table");
    DownloadSession downloadSession;
    try {
      if (context.getPartition() != null && !context.getPartition().isEmpty()) {
        PartitionSpec partitionSpec = new PartitionSpec(context.getPartition());
        downloadSession =
            tunnel.createDownloadSession(context.getProject(), context.getTable(), partitionSpec);
      } else {
        downloadSession = tunnel.createDownloadSession(context.getProject(), context.getTable());
      }
      context.setAction(downloadSession);
      LOG.info("start to create download");
      context.setRecordCount(downloadSession.getRecordCount());
      context.setSchema(downloadSession.getSchema());
      LOG.info("end to init RDTDownloader");
    } catch (TunnelException e) {
      LOG.error(e.getMessage(), e);
      throw new ROdpsException(e.getErrorCode() + e.getErrorMsg());
    }
    List<Object> ptkv = this.genPartitionCell();
    List<List<String>> ret = genTableSchema(ptkv == null ? null : (List<String>) ptkv.get(0));
    try {
      List<DownloadWorker> workers = this.createWorkerList(tempDataFile);
      String errorMessage = new String();
      LOG.info("wait for download end");
      for (DownloadWorker worker : workers) {
        worker.t.join(); // TODO: add time out here
        if (!worker.IsSuccessful()) {
          errorMessage += worker.getErrorMessage();
        }
      }
      if (0 < errorMessage.length()) {
        throw new ROdpsException(errorMessage);
      }
      ret.add(getFiles(tempDataFile, workers.size()));
    } catch (Exception e) {
      throw new IOException("down load table met InterruptedException", e);
    }
    return ret;
  }

  @Override
  public DownloadWorker createWorker(int threadId, Context<DownloadSession> context,
      long startRecordNumber, long downloadRecordNumber, String savePath) throws ROdpsException {
    File file = new File(savePath);
    if (file.exists()) {
      LOG.warn("download file: " + savePath + "already exist, now delete it.");
      file.delete();
    }
    return new DownloadWorker(threadId, context, startRecordNumber, downloadRecordNumber, savePath);
  }

  /**
   * 返回到R的schema
   * 
   * @Title: genTableSchema
   * @Description: TODO
   * @return
   * @return LinkedHashMap<String,String>
   * @throws
   */
  private List<List<String>> genTableSchema(List<String> keys) {
    List<List<String>> ret = new ArrayList<List<String>>();
    ret.add(new ArrayList<String>());
    ret.add(new ArrayList<String>());
    int columnNumber = this.context.getSchema().getColumns().size();
    for (int i = 0; i < columnNumber; ++i) {
      ret.get(0).add(this.context.getSchema().getColumn(i).getName());
      ret.get(1).add(
          this.context.getSchema().getColumn(i).getTypeInfo().getTypeName()
              .replace("ODPS_", "").toLowerCase());
    }
    if (keys != null) {
      for (String k : keys) {
        ret.get(0).add(k);
        ret.get(1).add("string");
      }
    }
    return ret;
  }


  /**
   * 生成partition值cell
   * 
   * @Title: genPartitionCell
   * @Description: TODO
   * @return
   * @throws ROdpsException
   * @return String
   * @throws
   */
  public List<Object> genPartitionCell() throws ROdpsException {
    if (this.context.getPartition() == null || this.context.getPartition().isEmpty()) {
      return null;
    }
    String[] ptcols = this.context.getPartition().split(",");
    StringBuffer vs = new StringBuffer();
    List<String> keys = new ArrayList<String>();
    List<Object> ret = new ArrayList<Object>();
    for (String p : ptcols) {
      String[] items = p.split("=");
      if (items.length != 2) {
        throw new ROdpsException("Partition express error:" + p);
      }
      if (vs.length() > 0) {
        vs.append(this.context.getColDim());
      }
      keys.add(items[0].trim().toLowerCase());
      vs.append("\"" + items[1].trim() + "\"");
    }
    ret.add(keys);
    ret.add(vs.toString());
    return ret;
  }

  public List<String> getFiles(String savePath, int fileNum) {
    ArrayList<String> fileNames = new ArrayList<String>();
    for (int i = 0; i < fileNum; i++) {
      fileNames.add(createTempFileName(savePath, i));
    }
    return fileNames;
  }
}
