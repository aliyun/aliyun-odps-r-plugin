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

package com.aliyun.odps.rodps;

import com.aliyun.odps.Instance;
import com.aliyun.odps.LogView;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Project;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableFilter;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.StsAccount;
import com.aliyun.odps.rodps.DataTunnel.Context;
import com.aliyun.odps.rodps.DataTunnel.DataFrameItem;
import com.aliyun.odps.rodps.DataTunnel.RDTDownloader;
import com.aliyun.odps.rodps.DataTunnel.RDTUploader;
import com.aliyun.odps.rodps.DataTunnel.ROdpsException;
import com.aliyun.odps.sqa.ExecuteMode;
import com.aliyun.odps.sqa.FallbackPolicy;
import com.aliyun.odps.sqa.SQLExecutor;
import com.aliyun.odps.sqa.SQLExecutorBuilder;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import java.io.BufferedReader;
import java.io.CharArrayWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ROdps {

  static Log LOG = LogFactory.getLog(ROdps.class);
  private String ODPS_PROJECT_NAME;
  private final Odps odps;
  private String DT_ENDPOINT;
  private static final int RETRY_MAX = 3;
  private static final String PROG_VERSION = "rodps-1.3";
  private String LOGVIEW_HOST;
  private HashMap<String, String> settings;

  public ROdps(
      String projectName,
      String accessID,
      String accessKey,
      String endPoint,
      String dtEndpoint,
      String logviewHost,
      String log4j_properties)
      throws ROdpsException {
    this(projectName, accessID, accessKey, "", endPoint, dtEndpoint, logviewHost, log4j_properties);
  }

  public ROdps(
      String projectName,
      String accessID,
      String accessKey,
      String stsToken,
      String endPoint,
      String dtEndpoint,
      String logviewHost,
      String log4j_properties)
      throws ROdpsException {

    if (log4j_properties == null || log4j_properties.isEmpty())
      LOG = LogFactory.getLog(ROdps.class);
    else {
      try {
        ConfigurationSource source = new ConfigurationSource(new FileInputStream(log4j_properties));
        Configurator.initialize(null, source);
      } catch (IOException e) {
        throw new ROdpsException("Invalid log4j property file");
      }
      LOG = LogFactory.getLog(ROdps.class);
    }
    LOG.info("Init Odps");
    if (projectName.equals("NA")
        || accessID.equals("NA")
        || accessKey.equals("NA")
        || endPoint.equals("NA")) {
      throw new ROdpsException("No project/accessID/accessKey/endPoint");
    }

    // trim input strings
    projectName = projectName.trim();
    accessID = accessID.trim();
    accessKey = accessKey.trim();
    endPoint = endPoint.trim();
    if (dtEndpoint == null || dtEndpoint.equals("NA")) {
      DT_ENDPOINT = null;
    } else {
      DT_ENDPOINT = dtEndpoint.trim();
      LOG.info("use specified tunnel endpoint : " + DT_ENDPOINT);
    }

    ODPS_PROJECT_NAME = projectName;
    LOGVIEW_HOST = logviewHost;

    if (stsToken == null || stsToken.length() <= 0) {
      odps = new Odps(new AliyunAccount(accessID, accessKey));
    } else {
      odps = new Odps(new StsAccount(accessID, accessKey, stsToken.trim()));
    }
    odps.setEndpoint(endPoint);
    odps.setDefaultProject(projectName);
    odps.setUserAgent(PROG_VERSION);
    if (logviewHost == null || logviewHost.equals("NA")) {
      LOGVIEW_HOST = new LogView(odps).getLogViewHost();
    } else {
      LOGVIEW_HOST = logviewHost.trim();
    }

    settings = new LinkedHashMap<>();
  }

  public void setBizId(String s) {
    set("biz_id", s);
  }

  public void set(String key, String value) {
    settings.put(key, value);
  }

  public void unset(String key) {
    settings.remove(key);
  }

  // use tunnel sdk to upload table
  public void writeTableFromDT(
      String projectName,
      String tableName,
      String partition,
      String dataFilePathName,
      String columnDelimiter,
      String rowDelimiter,
      long recordCount,
      int threadNumber)
      throws ROdpsException {

    int retryTimes = 0;
    while (true) {
      try {
        LOG.info("before create RDTUploader");
        if (projectName == null) {
          projectName = this.ODPS_PROJECT_NAME;
        }
        if (partition != null) {
          partition = formatPartition(partition, "", ",");
        }
        Context<UploadSession> context =
            new Context<UploadSession>(
                odps,
                DT_ENDPOINT,
                projectName,
                tableName,
                partition,
                -1,
                columnDelimiter,
                rowDelimiter,
                threadNumber);
        context.setRecordCount(recordCount);
        RDTUploader uploader = new RDTUploader(context);
        uploader.upload(dataFilePathName);
        return;
      } catch (IOException e) {
        if (++retryTimes <= RETRY_MAX) {
          LOG.error(
              "write table encounter exception:"
                  + e.getMessage()
                  + ", retry times = "
                  + retryTimes);
          try {
            Thread.sleep(5000);
          } catch (InterruptedException e1) {
            LOG.error("Sleep interrupted!", e1);
          }
          continue;
        }
        throw new ROdpsException(e);
      } catch (Exception e) {
        throw new ROdpsException(e);
      }
    }
  }

  /*
   * *use tunnel sdk to load table from odps *
   */
  public List<List<String>> loadTableFromDT(
      String projectName,
      String tableName,
      String partition,
      String tempFile,
      String colDelimiter,
      String rowDelimiter,
      int limit,
      int threadNumber)
      throws ROdpsException {

    int retryTimes = 0;
    while (true) {
      try {

        if (projectName == null) {
          projectName = ODPS_PROJECT_NAME;
        }
        if (partition != null) {
          partition = formatPartition(partition, "", ",");
        }
        Context<DownloadSession> context =
            new Context<DownloadSession>(
                odps,
                DT_ENDPOINT,
                projectName,
                tableName,
                partition,
                limit,
                colDelimiter,
                rowDelimiter,
                threadNumber);
        RDTDownloader downloader = new RDTDownloader(context);
        return downloader.downloadTable(tempFile);
      } catch (IOException e) {
        if (++retryTimes <= RETRY_MAX) {
          LOG.error(
              "load table encounter exception:" + e.getMessage() + ", retry times = " + retryTimes);
          e.printStackTrace();
          try {
            Thread.sleep(5000);
          } catch (InterruptedException e1) {
            LOG.error("Sleep interrupted!", e1);
          }
          continue;
        }
        throw new ROdpsException(e);
      } catch (Exception e) {
        throw new ROdpsException(e);
      }
    }
  }

  /**
   * @title: Use Project
   * @description: use project 命令
   * @param projectName
   * @return boolean
   * @throws ROdpsException
   */
  public boolean useProject(String projectName) throws ROdpsException {
    if (projectName == null) {
      throw new ROdpsException("ProjectName is null");
    }
    projectName = projectName.trim().toLowerCase();
    this.runSqlTask("use " + projectName);
    this.ODPS_PROJECT_NAME = projectName;
    return true;
  }

  /**
   * @title: createSchema
   * @description: 将json转化成name:type的map
   * @param schemaJson
   * @param type
   * @throws ROdpsException
   * @return Map<String,Schema>
   */
  private Map<String, Schema> createSchema(String schemaJson, String type) throws ROdpsException {
    Map<String, Schema> ret = new LinkedHashMap<String, Schema>();
    try {
      JSONObject jsonMap = new JSONObject(schemaJson);
      if (jsonMap.get(type) != null) {
        JSONArray jsonArray = jsonMap.getJSONArray(type);
        for (int i = 0; i < jsonArray.length(); i++) {
          JSONObject col = (JSONObject) (jsonArray.get(i));
          Schema schema =
              new Schema(
                  col.getString("name"),
                  col.getString("type"),
                  (col.has("comment") ? col.getString("comment") : null));
          schema.setPartitionKey(type.equals("partitionKeys"));
          ret.put(col.getString("name"), schema);
        }
      }
      return ret;
    } catch (Exception e) {
      LOG.error(e);
      throw new ROdpsException(e);
    }
  }

  /**
   * @title: Get Project Object
   * @description: 根据projectName创建Project对象
   * @param projectName
   * @return Project
   * @throws OdpsException
   */
  private Project getProjectObject(String projectName) throws OdpsException {
    if (projectName == null
        || projectName.isEmpty()
        || projectName.equals(this.ODPS_PROJECT_NAME)) {
      Project p = odps.projects().get(ODPS_PROJECT_NAME);
      p.reload();
      return p;
    } else {
      // return new Project(client, projectName);
      Project TempProject = odps.projects().get(projectName);
      TempProject.reload();
      return TempProject;
    }
  }

  /**
   * @title: getProjectName
   * @description: 根据projectName是否为null返回projectName
   * @param projectName
   * @return String
   */
  public String getProjectName(String projectName) {
    if (projectName == null
        || projectName.isEmpty()
        || projectName.equals(this.ODPS_PROJECT_NAME)) {
      return this.ODPS_PROJECT_NAME;
    } else {
      return projectName;
    }
  }

  /**
   * @title: getTableSize
   * @description: Get table size in bytes
   * @param tableName
   * @return long
   * @throws ROdpsException
   */
  public long getTableSize(String projectName, String tableName, String partition) {
    Table tbl = odps.tables().get(this.getProjectName(projectName), tableName);
    return tbl.getSize();
  }

  /**
   * @title: DescribeTable
   * @description: TODO
   * @param projectName
   * @param tableName
   * @param partition
   * @return List<DataFrameItem>
   * @throws ROdpsException
   * @throws CloneNotSupportedException
   */
  public List<DataFrameItem> describeTable(String projectName, String tableName, String partition)
      throws ROdpsException {
    Table tbl = odps.tables().get(this.getProjectName(projectName), tableName);
    List<DataFrameItem> ps = new ArrayList<DataFrameItem>();
    if (partition != null) {
      partition = this.formatPartition(partition, "'", ",");
    }
    try {
      tbl.reload();
      ps.add(this.createSingleValueFrame("owner", "String", tbl.getOwner()));
      ps.add(this.createSingleValueFrame("project", "String", tbl.getProject()));
      ps.add(this.createSingleValueFrame("comment", "String", tbl.getComment()));
      ps.add(
          this.createSingleValueFrame(
              "create_time", "DateTime", formatDateTime(tbl.getCreatedTime())));
      ps.add(
          this.createSingleValueFrame(
              "last_modified_time", "DateTime", formatDateTime(tbl.getLastMetaModifiedTime())));
      ps.add(this.createSingleValueFrame("is_internal_table", "boolean", tbl.isVirtualView()));
      if (tbl.isVirtualView()) {
        long size = tbl.getPhysicalSize();
        if (partition == null || partition.isEmpty()) {
          ps.add(this.createSingleValueFrame("size", "Long", size));
        } else {
          ps.add(this.createSingleValueFrame("partition_size", "Long", size));
          ps.add(this.createSingleValueFrame("partition_name", "String", partition));
        }
      }
      Map<String, Schema> columns = this.createSchema(tbl.getJsonSchema(), "columns");
      DataFrameItem item = new DataFrameItem("columns", "string");
      for (Map.Entry<String, Schema> entry : columns.entrySet()) {
        item.getData().add(entry.getValue().toString());
      }
      ps.add(item);
      Map<String, Schema> ptKeys = this.createSchema(tbl.getJsonSchema(), "partitionKeys");
      if (ptKeys != null && ptKeys.size() > 0) {
        DataFrameItem ptItem = new DataFrameItem("partition_keys", "String");
        for (Map.Entry<String, Schema> entry : ptKeys.entrySet()) {
          ptItem.getData().add(entry.getValue().toString());
        }
        ps.add(ptItem);
      }
      return ps;
    } catch (Exception e) {
      LOG.error(e);
      throw new ROdpsException(e);
    }
  }

  private String formatDateTime(Date date) {
    java.text.SimpleDateFormat format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    return format.format(date);
  }

  private DataFrameItem createSingleValueFrame(String name, String type, Object obj) {
    DataFrameItem item = new DataFrameItem(name, type);
    if (obj instanceof List) {
      item.setData((List) obj);
    } else {
      item.getData().add(obj);
    }
    return item;
  }

  /**
   * @title: DropTable
   * @param projectName
   * @param tableName
   * @return boolean
   * @throws ROdpsException
   */
  public boolean dropTable(String projectName, String tableName) throws ROdpsException {
    try {
      this.runSqlTask(
          "drop table " + getTableName(this.getProjectName(projectName), tableName) + ";");
      return true;
    } catch (Exception e) {
      LOG.error(e);
      throw new ROdpsException(e);
    }
  }

  /**
   * @title: isTableExist
   * @param tableName
   * @return boolean
   * @throws ROdpsException
   */
  public boolean isTableExist(String projectName, String tableName, String partition)
      throws ROdpsException {
    if (partition != null) {
      partition = formatPartition(partition, "'", ",");
    }
    // Table table = new Table(this.getProjectObject(projectName), tableName);
    // Table table = odps.tables().get(projectName,tableName);
    Table table = odps.tables().get(this.getProjectName(projectName), tableName);
    try {
      if (partition == null || partition.isEmpty()) {
        table.reload();
        return true;
      } else {
        // List<String> pts = table.listPartitions();
        // return pts!=null && pts.size()>0 && pts.contains(partition);
        return table.hasPartition(new PartitionSpec(partition));
      }
    } catch (OdpsException e) {
      if (e.getMessage().indexOf("Table not found") > 0) {
        return false;
      }
      LOG.error(e);
      throw new ROdpsException(e);
    }
  }

  /**
   * @title: getTableSchemaJson
   * @description: 以Json字符串格式取得一个指定Table的Schema
   * @param projectName
   * @param tableName
   */
  public String getTableSchemaJson(String projectName, String tableName) {
    String tableSchemaJson;
    // Table table = new Table(this.getProjectObject(projectName), tableName);
    Table table = odps.tables().get(this.getProjectName(projectName), tableName);

    try {
      table.reload();
      tableSchemaJson = table.getJsonSchema();
    } catch (OdpsException e) {
      tableSchemaJson = e.getMessage();
    }
    return tableSchemaJson;
  }

  /**
   * @title: getIndexFromColName
   * @description: 通过列名获取该列在Table Schema中的Index
   * @param colName
   * @param tableSchemaJson
   */
  public int getIndexFromColName(String colName, String tableSchemaJson) {
    if (0 >= tableSchemaJson.length()) {
      return -1;
    }
    try {
      JSONObject schema = new JSONObject(tableSchemaJson);
      JSONArray columns = schema.getJSONArray("columns");
      for (int i = 0; i < columns.length(); ++i) {
        JSONObject column = (JSONObject) columns.get(i);
        String columnName = (String) column.get("name");
        if (colName.equals(columnName)) {
          return i + 1;
        }
      }
      return -1;
    } catch (JSONException e) {
      return -1;
    }
  }

  public List<String> runSqlTask(String sql) throws ROdpsException {
    return runSqlTask(sql, false);
  }

  /**
   * @title: runSqlTask
   * @description: TODO
   * @param sql The SQL string
   * @param interactive enable interactive (MCQA) or not
   * @return List<String>
   * @throws ROdpsException
   */
  public List<String> runSqlTask(String sql, boolean interactive) throws ROdpsException {
    // If the client forgets to end with a semicolon, append it.
    if (!sql.contains(";")) {
      sql += ";";
    }

    LOG.debug("sql: " + sql);

    // Create instance
    Instance inst;
    String TASK_NAME = "rodps_sql_task";
    try {
      if (interactive) {
        TASK_NAME = "rodps_mcqa_task";
        SQLExecutorBuilder builder = SQLExecutorBuilder.builder();
        SQLExecutor sqlExecutor = null;
        sqlExecutor =
            builder
                .odps(odps)
                .executeMode(ExecuteMode.INTERACTIVE)
                .fallbackPolicy(FallbackPolicy.alwaysFallbackPolicy())
                .build();
        if (settings.containsKey("odps.sql.submit.mode")) {
          settings.put("odps.sql.submit.mode", "script");
        }
        sqlExecutor.run(sql, settings);
        inst = sqlExecutor.getInstance();
      } else {
        inst = SQLTask.run(odps, odps.getDefaultProject(), sql, TASK_NAME, settings, null);
      }

      LogView logView = new LogView(odps);
      if (LOGVIEW_HOST != null) {
        logView.setLogViewHost(LOGVIEW_HOST);
      }
      String logViewUrl = logView.generateLogView(inst, 7 * 24);
      System.err.println(logViewUrl);

      inst.waitForSuccess();
      Map<String, String> results = inst.getTaskResults();
      String result = results.get(TASK_NAME);
      if (result == null || result.isEmpty()) {
        return new ArrayList<String>();
      }
      return new ArrayList<String>(Arrays.asList(results.get(TASK_NAME).split("\n")));
    } catch (Exception e) {
      LOG.error("runSqlTask error, sql=" + sql, e);
      throw new ROdpsException(e);
    }
  }

  private String getTableName(String projectName, String tableName) throws ROdpsException {
    if (tableName == null || tableName.isEmpty()) {
      throw new ROdpsException("tableName is empty");
    }
    return (projectName == null || projectName.isEmpty() ? "" : (projectName + ".")) + tableName;
  }

  public class Schema {

    public Schema(String name, String type, String comment) {
      this.name = name;
      this.type = type;
      this.comment = comment;
    }

    private String name;
    private String type;
    private String comment;
    private boolean isPartitionKey;

    public String getName() {

      return name;
    }

    public void setName(String name) {

      this.name = name;
    }

    public String getType() {

      return type;
    }

    public void setType(String type) {

      this.type = type;
    }

    public String getComment() {

      return comment;
    }

    public void setComment(String comment) {

      this.comment = comment;
    }

    public boolean isPartitionKey() {

      return isPartitionKey;
    }

    public void setPartitionKey(boolean isPartitionKey) {
      this.isPartitionKey = isPartitionKey;
    }

    public String toString() {
      return name + "|" + type + "|" + (comment != null ? comment : "");
    }
  }

  /**
   * @title: getTables
   * @return List<DataFrameItem>
   */
  public List<DataFrameItem> getTables(String projectName, String pattern) {
    DataFrameItem<String> owner = new DataFrameItem<String>("owner", "string");
    DataFrameItem<String> tableName = new DataFrameItem<String>("table_name", "string");
    List<DataFrameItem> data = new ArrayList<DataFrameItem>();
    data.add(owner);
    data.add(tableName);

    TableFilter filter = new TableFilter();
    filter.setName(pattern);

    for (Iterator<Table> it = odps.tables().iterator(projectName, filter); it.hasNext(); ) {
      Table tb = it.next();
      owner.getData().add(tb.getOwner());
      tableName.getData().add(tb.getName());
    }
    return data;
  }

  public static String formatPartition(String part, String valueDim, String fieldDim)
      throws ROdpsException {
    LinkedHashMap<String, String> kv = parsePartition(part);
    return partitionMap2String(kv, valueDim, fieldDim);
  }

  /**
   * @title: parsePartition
   * @description: 解析partition
   * @param part
   * @return LinkedHashMap<String,String>
   * @throws ROdpsException
   */
  private static LinkedHashMap<String, String> parsePartition(String part) throws ROdpsException {
    LinkedHashMap<String, String> ret = new LinkedHashMap<String, String>();
    String[] pts = part.split(",|/");
    for (String p : pts) {
      String[] kv = p.split("=");
      if (kv.length != 2) {
        throw new ROdpsException("Partition expression error:" + part);
      }
      if (kv[1].startsWith("'") && kv[1].endsWith("'")
          || kv[1].startsWith("\"") && kv[1].endsWith("\"")) {
        kv[1] = kv[1].substring(1, kv[1].length() - 1);
      }
      ret.put(kv[0], kv[1]);
    }
    return ret;
  }

  /**
   * @title: partitionMap2String
   * @description: TODO
   * @param spec PartitionSpec
   * @param valueDim 值的分隔符
   * @param fieldDim　字段间的分隔符
   * @return String
   */
  private static String partitionMap2String(
      Map<String, String> spec, String valueDim, String fieldDim) {
    StringBuffer ret = new StringBuffer();
    for (Map.Entry<String, String> entry : spec.entrySet()) {
      if (ret.length() > 0) {
        ret.append(fieldDim);
      }
      ret.append(entry.getKey() + "=" + valueDim + entry.getValue() + valueDim);
    }
    return ret.toString();
  }

  /**
   * @title: setLogPath
   * @description: set log path
   */
  public boolean setLogPath(String log_path) {
    String fileName = ROdps.class.getClassLoader().getResource("log4j.properties").getPath();
    String mode = "loghome";
    File file = new File(fileName);
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader(file));
      CharArrayWriter tempStream = new CharArrayWriter();
      String tempString = null;
      int line = 1;
      while ((tempString = reader.readLine()) != null) {
        if (tempString.contains(mode) && (!tempString.contains("${" + mode + "}"))) {
          tempString = tempString.substring(0, tempString.indexOf('=') + 1) + log_path;
        }
        tempStream.write(tempString);
        tempStream.append(System.getProperty("line.separator"));
      }
      reader.close();
      FileWriter out = new FileWriter(fileName);
      tempStream.writeTo(out);
      out.close();

    } catch (IOException e) {
      e.printStackTrace();
      return false;
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e1) {
        }
      }
    }
    return true;
  }
}
