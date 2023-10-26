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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;

import org.sqlite.SQLiteConfig;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.IntervalDayTime;
import com.aliyun.odps.data.IntervalYearMonth;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.type.TypeInfo;

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
   * @param reader               DT reader
   * @param downloadRecordNumber download record number
   * @throws Exception
   */
  public long readDataTunnel(RecordReader reader, long downloadRecordNumber) throws Exception {
    ArrayRecord record;
    long loadedRecordNum = 0;
    int batchSize = 10000;
    int columnNumber = context.getSchema().getColumns().size();
    int allColNumber = columnNumber;
    if (ptks != null) {
      allColNumber += ptks.size();
    }
    createTable();
    if (downloadRecordNumber == 0)
      return 0L;

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
        record = (ArrayRecord) reader.read();
        int i = 0;
        for (; i < columnNumber; i++) {
          TypeInfo colType = context.getSchema().getColumn(i).getTypeInfo();
          switch (colType.getOdpsType()) {
            case BOOLEAN: {
              Boolean v = record.getBoolean(i);
              if (v == null)
                insPreStmt.setNull(i + 1, Types.BOOLEAN);
              else
                insPreStmt.setBoolean(i + 1, v);
              break;
            }
            case BIGINT: {
              // XXX: int of r is 32bit,so convert int to double
              Long v = record.getBigint(i);
              if (v == null)
                insPreStmt.setNull(i + 1, Types.DOUBLE);
              else
                insPreStmt.setDouble(i + 1, v);
              break;
            }
            case INT: {
              Integer v = record.getInt(i);
              if (v == null)
                insPreStmt.setNull(i + 1, Types.DOUBLE);
              else
                insPreStmt.setDouble(i + 1, v);
              break;
            }
            case TINYINT: {
              Byte v = record.getTinyint(i);
              if (v == null)
                insPreStmt.setNull(i + 1, Types.DOUBLE);
              else
                insPreStmt.setDouble(i + 1, v);
              break;
            }
            case SMALLINT: {
              Short v = record.getSmallint(i);
              if (v == null)
                insPreStmt.setNull(i + 1, Types.DOUBLE);
              else
                insPreStmt.setDouble(i + 1, v);
              break;
            }
            case DOUBLE: {
              Double v = record.getDouble(i);
              if (v == null)
                insPreStmt.setNull(i + 1, Types.DOUBLE);
              else
                insPreStmt.setDouble(i + 1, v);
              break;
            }
            case FLOAT: {
              Float v = record.getFloat(i);
              if (v == null)
                insPreStmt.setNull(i + 1, Types.DOUBLE);
              else
                insPreStmt.setDouble(i + 1, v);
              break;
            }
            case DATETIME: {
              java.util.Date v = record.getDatetime(i);
              if (v == null)
                insPreStmt.setNull(i + 1, Types.DOUBLE);
              else
                insPreStmt.setDouble(i + 1, v.getTime() / 1000.0);
              break;
            }
            case DATE: {
              java.sql.Date v = record.getDate(i);
              if (v == null)
                insPreStmt.setNull(i + 1, Types.DOUBLE);
              else
                insPreStmt.setDouble(i + 1, v.getTime() / 1000.0);
              break;
            }
            case TIMESTAMP: {
              Timestamp v = record.getTimestamp(i);
              if (v == null)
                insPreStmt.setNull(i + 1, Types.DOUBLE);
              else
                insPreStmt.setDouble(i + 1, v.getTime() / 1000.0);
              break;
            }
            case DECIMAL: {
              BigDecimal v = record.getDecimal(i);
              if (v == null)
                insPreStmt.setNull(i + 1, Types.NULL);
              else
                insPreStmt.setString(i + 1, v.toPlainString());
              break;
            }
            case STRING: {
              String v = record.getString(i);
              if (v == null)
                insPreStmt.setNull(i + 1, Types.NULL);
              else
                insPreStmt.setString(i + 1, v);
              break;
            }
            case CHAR: {
              Char v = record.getChar(i);
              if (v == null)
                insPreStmt.setNull(i + 1, Types.NULL);
              else
                insPreStmt.setString(i + 1, v.getValue());
              break;
            }
            case VARCHAR: {
              Varchar v = record.getVarchar(i);
              if (v == null)
                insPreStmt.setNull(i + 1, Types.NULL);
              else
                insPreStmt.setString(i + 1, v.getValue());
              break;
            }
            case BINARY: {
              byte[] v = record.getBytes(i);
              if (v == null)
                insPreStmt.setNull(i + 1, Types.NULL);
              else
                insPreStmt.setBytes(i + 1, v);
              break;
            }
            case INTERVAL_YEAR_MONTH: {
              IntervalYearMonth v = record.getIntervalYearMonth(i);
              if (v == null)
                insPreStmt.setNull(i + 1, Types.DOUBLE);
              else
                insPreStmt.setDouble(i + 1, v.getTotalMonths());
              break;
            }
            case INTERVAL_DAY_TIME: {
              IntervalDayTime v = record.getIntervalDayTime(i);
              if (v == null)
                insPreStmt.setNull(i + 1, Types.DOUBLE);
              else
                insPreStmt.setDouble(i + 1, v.getTotalSeconds());
              break;
            }
            case MAP:
            case STRUCT:
            case ARRAY:
            default:
              throw new ROdpsException("Unsupported type " + colType.getTypeName());
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
      return loadedRecordNum;
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
  public long writeDataTunnel(RecordWriter writer) throws Exception {
    Statement stmt = null;
    ResultSet rs = null;
    try {
      stmt = this.conn.createStatement();
      String sql = "select * from [" + context.getTable() + "]";
      rs = stmt.executeQuery(sql);
      long i = 0;
      while (rs.next()) {
        ArrayRecord bufRecord = (ArrayRecord) ((UploadSession) (context.getAction())).newRecord();
        for (int j = 0; j < this.context.getSchema().getColumns().size(); j++) {
          if (rs.getObject(j + 1) == null) {
            bufRecord.set(j, null);
            continue;
          }
          TypeInfo colType = this.context.getSchema().getColumn(j).getTypeInfo();
          switch (colType.getOdpsType()) {
            case BOOLEAN:
              bufRecord.setBoolean(j, rs.getBoolean(j + 1));
              break;
            case BIGINT:
              bufRecord.setBigint(j, (long) rs.getDouble(j + 1));
              break;
            case INT:
              bufRecord.setInt(j, (int) rs.getDouble(j + 1));
              break;
            case TINYINT:
              bufRecord.setTinyint(j, (byte) rs.getDouble(j + 1));
              break;
            case SMALLINT:
              bufRecord.setSmallint(j, (short) rs.getShort(j + 1));
              break;
            case DOUBLE:
              bufRecord.setDouble(j, rs.getDouble(j + 1));
              break;
            case FLOAT:
              bufRecord.setFloat(j, (float) rs.getDouble(j + 1));
              break;
            case DATETIME:
              bufRecord.setDatetime(j, new java.util.Date((long) (rs.getDouble(j + 1) * 1000.0)));
              break;
            case DATE:
              bufRecord.setDate(j, new java.sql.Date((long) (rs.getDouble(j + 1) * 1000.0)));
              break;
            case TIMESTAMP:
              bufRecord.setTimestamp(j, new Timestamp((long) (rs.getDouble(j + 1) * 1000.0)));
              break;
            case DECIMAL:
              bufRecord.setDecimal(j, new BigDecimal(rs.getString(j + 1)));
              break;
            case STRING:
              bufRecord.setString(j, rs.getString(j + 1));
              break;
            case CHAR:
              bufRecord.setChar(j, new Char(rs.getString(j + 1)));
              break;
            case VARCHAR:
              bufRecord.setVarchar(j, new Varchar(rs.getString(j + 1)));
              break;
            case BINARY:
              bufRecord.setBinary(j, new Binary(rs.getBytes(j + 1)));
              break;
            case INTERVAL_YEAR_MONTH:
              bufRecord.setIntervalYearMonth(j, new IntervalYearMonth((int) rs.getDouble(j + 1)));
              break;
            case INTERVAL_DAY_TIME:
              bufRecord.setIntervalDayTime(j, new IntervalDayTime((int) rs.getDouble(j + 1), 0));
              break;
            case MAP:
            case ARRAY:
            case STRUCT:
            default:
              throw new ROdpsException("Unsupported type " + colType.getTypeName());
          }
        }
        i++;
        writer.write(bufRecord);
      }
      return i;
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
   */
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
   */
  private void createTable() throws Exception {
    TableSchema schema = context.getSchema();
    int columnNumber = schema.getColumns().size();
    StringBuffer sb = new StringBuffer("create table [" + context.getTable() + "] (");
    for (int i = 0; i < columnNumber; ++i) {
      String colName = "[" + context.getSchema().getColumn(i).getName() + "]";
      sb.append(colName);
      sb.append(" ");
      TypeInfo colType = context.getSchema().getColumn(i).getTypeInfo();
      String type;
      switch (colType.getOdpsType()) {
        case BOOLEAN:
          type = "boolean";
          break;
        case BIGINT:
        case INT:
        case TINYINT:
        case SMALLINT:
        case DOUBLE:
        case FLOAT:
        case DATETIME:
        case DATE:
        case TIMESTAMP:
          type = "double";
          break;
        case DECIMAL:
        case STRING:
        case CHAR:
        case VARCHAR:
        case BINARY:
          type = "text";
          break;
        case MAP:
        case STRUCT:
        case ARRAY:
        default:
          throw new ROdpsException("Unsupported type " + colType.getTypeName());
      }
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
