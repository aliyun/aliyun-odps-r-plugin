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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import org.sqlite.SQLiteConfig;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;

public class SqliteMiddleStorage<T> implements MiddleStorage {
  private final Connection conn;
  private final Context<T> context;
  private ArrayList<String> ptks;
  private ArrayList<String> ptvs;

  public SqliteMiddleStorage(String dbName, Context<T> context) throws ROdpsException {
    this.context = context;
    try {
      Class.forName("org.sqlite.JDBC");
      SQLiteConfig config = new SQLiteConfig();
      config.setSynchronous(SQLiteConfig.SynchronousMode.OFF);
      this.conn = DriverManager.getConnection("jdbc:sqlite:" + dbName, config.toProperties());
    } catch (Exception e) {
      throw new ROdpsException(e, "Initial Sqlite Connection fail!");
    }

    initialPtkvs(context.getPartition());
  }

  /**
   * Read odps data using DT, and persist these data into sqlite
   * 
   * @param reader DT reader
   * @param downloadRecordNumber download record number
   * @throws Exception
   */
  public void saveDtData(RecordReader reader, long downloadRecordNumber) throws Exception {
    Record record;
    long loadedRecordNum = 0;
    int batchSize = 10000;
    int columnNumber = context.getSchema().getColumns().size();
    int allColNumber = columnNumber;
    if (ptks != null) {
      allColNumber += ptks.size();
    }
    createTable();
    if (downloadRecordNumber == 0)
      return;

    // create insert sql
    String insSql = "insert into [" + context.getTable() + "] values(";
    for (int i = 0; i < allColNumber; i++) {
      insSql += "?,";
    }
    insSql = insSql.substring(0, insSql.length() - 1) + ");";
    PreparedStatement insPreStmt = null;

    try {
      insPreStmt = this.conn.prepareStatement(insSql);

      while (loadedRecordNum < downloadRecordNumber) {
        record = reader.read();
        int i = 0;
        for (; i < columnNumber; i++) {
          Object v = null;
          switch (context.getSchema().getColumn(i).getType()) {
          // int of r is 32bit,so convert int to double
            case BIGINT:
              v = record.getBigint(i);
              if (v == null)
                insPreStmt.setNull(i + 1, Types.DOUBLE);
              else
                insPreStmt.setDouble(i + 1, new Double((Long) v));
              break;
            case BOOLEAN:
              v = record.getBoolean(i);
              if (v == null)
                insPreStmt.setNull(i + 1, Types.BOOLEAN);
              else
                insPreStmt.setBoolean(i + 1, (Boolean) v);
              break;
            case DATETIME:
              v = record.getDatetime(i);
              if (v == null)
                insPreStmt.setNull(i + 1, Types.DOUBLE);
              else
                insPreStmt.setDouble(i + 1, new Double(((java.util.Date) v).getTime() / 1000));
              break;
            case DOUBLE:
              v = record.getDouble(i);
              if (v == null)
                insPreStmt.setNull(i + 1, Types.DOUBLE);
              else
                insPreStmt.setDouble(i + 1, (Double) v);
              break;
            case STRING:
              v = record.getString(i);
              if (v == null)
                insPreStmt.setNull(i + 1, Types.NULL);
              else
                insPreStmt.setString(i + 1, (String) v);
              break;
            default:
              throw new ROdpsException("Unknown data type!");
          }
        }

        // add partition values
        if (ptvs != null) {
          for (String v : ptvs) {
            i++;
            insPreStmt.setString(i, v);
          }
        }
        insPreStmt.addBatch();
        loadedRecordNum++;
        if (loadedRecordNum % batchSize == 0) {
          this.conn.setAutoCommit(false);
          insPreStmt.executeBatch();
          this.conn.commit();
          insPreStmt.clearBatch();
        }
      }

      this.conn.setAutoCommit(false);
      insPreStmt.executeBatch();
      this.conn.commit();
    } finally {
      if (insPreStmt != null) {
        insPreStmt.close();
      }
    }
  }

  /**
   * Read data from sqlite, and write these data into odps using DT
   * 
   * @param writer DT writer
   * @throws Exception
   */
  public void writeToDt(RecordWriter writer) throws Exception {
    Statement stmt = null;
    ResultSet rs = null;
    try {
      stmt = this.conn.createStatement();
      String sql = "select * from [" + context.getTable() + "]";
      rs = stmt.executeQuery(sql);
      long i = 0;
      while (rs.next()) {
        Record bufRecord = ((UploadSession) (context.getAction())).newRecord();
        for (int j = 0; j < this.context.getSchema().getColumns().size(); j++) {
          boolean isnull = rs.getObject(j + 1) == null;
          switch (this.context.getSchema().getColumn(j).getType()) {
            case BIGINT:
              bufRecord.setBigint(j, isnull ? null : rs.getLong(j + 1));
              break;
            case BOOLEAN:
              bufRecord.setBoolean(j, isnull ? null : rs.getBoolean(j + 1));
              break;
            case DATETIME:
              bufRecord.setDatetime(j, isnull ? null : new java.util.Date(rs.getTimestamp(j + 1)
                  .getTime() * 1000));
              break;
            case DOUBLE:
              bufRecord.setDouble(j, isnull ? null : rs.getDouble(j + 1));
              break;
            case STRING:
              bufRecord.setString(j, rs.getString(j + 1));
              break;
            default:
              throw new ROdpsException("Unknown data type!");
          }
        }
        i++;
        writer.write(bufRecord);
      }
    } finally {
      if (rs != null) {
        rs.close();
      }
      if (stmt != null) {
        stmt.close();
      }
    }
  }

  /**
   * close database connection
   * */
  public void close() {
    if (this.conn != null) {
      try {
        this.conn.close();
      } catch (SQLException e) {
      }
    }
  }

  /**
   * Create table in sqlite. The schema is as same as odps table.
   * 
   * @throws Exception
   * */
  private void createTable() throws Exception {
    TableSchema schema = context.getSchema();
    int columnNumber = schema.getColumns().size();
    StringBuffer sb = new StringBuffer("create table [" + context.getTable() + "] (");
    for (int i = 0; i < columnNumber; ++i) {
      String colName = "[" + context.getSchema().getColumn(i).getName() + "]";
      String colType =
          context.getSchema().getColumn(i).getType().toString().replace("ODPS_", "").toLowerCase();
      sb.append(colName);
      sb.append(" ");

      String type = colType;
      if (colType.equals("string"))
        type = "text";
      else if (colType.equals("bigint") || colType.equals("double") || colType.equals("datetime"))
        type = "double";
      else if (colType.equals("boolean"))
        type = "boolean";
      else
        throw new ROdpsException("Unregonized type " + colType);

      sb.append(type);
      sb.append(",");
    }

    // contain partition columns
    if (this.ptks != null) {
      for (String key : this.ptks) {
        sb.append(key + " text,");
      }
    }
    String sql = sb.toString();
    sql = sql.substring(0, sql.length() - 1) + ")";
    // create table in sqlite
    Statement stmt = this.conn.createStatement();
    stmt.executeUpdate(sql);
    stmt.close();
  }

  /**
   * parse partition to key list and value list
   * 
   * @param part partition, format:key=value,...
   */
  private void initialPtkvs(String part) throws ROdpsException {
    if (part == null) {
      return;
    }

    this.ptks = new ArrayList<String>();
    this.ptvs = new ArrayList<String>();
    String[] pts = part.split(",");
    for (String p : pts) {
      String[] kv = p.split("=");
      if (kv.length != 2) {
        throw new ROdpsException("Partition expression error:" + part);
      }
      this.ptks.add(kv[0]);
      this.ptvs.add(kv[1]);
    }
    return;
  }
}
