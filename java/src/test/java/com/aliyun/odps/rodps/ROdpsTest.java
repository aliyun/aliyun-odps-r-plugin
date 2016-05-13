/*
 * Copyright 1999-2015 Alibaba Group Holding Ltd.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * @Title: ROdpsTest.java
 * @Package com.aliyun.odps.rodps.UnitTest
 * @Description: TODO(用一句话描述该文件做什么)
 * @author dendi.ywd
 * @date 2015-8-10 09:11:38
 * @version V1.0
 */
package com.aliyun.odps.rodps;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.rodps.DataTunnel.DataFrameItem;
import com.aliyun.odps.rodps.DataTunnel.ROdpsException;

import junit.framework.TestCase;

/**
 * @Title: ROdpsTest.java
 * @Package com.aliyun.odps.rodps.UnitTest
 * @Description: TODO(添加描述)
 * @author dendi.ywd
 * @date 2015-8-10 09:11:38
 * @version V1.0
 */

public class ROdpsTest extends TestCase {
  final static String table = "odps_r_operator";
  static String file;
  static ROdps rodps;

  protected void setUp() throws ROdpsException, OdpsException, ClassNotFoundException {
    String odps_config_path = ROdpsTest.class.getClassLoader().getResource("odps_config.ini").getPath();
    Map<String, String> conf = loadConfig(odps_config_path);
    if (conf == null) {
      System.exit(-1);
    }
    file = conf.get("sqlite_temp") + table;
    rodps = new ROdps(conf.get("project_name"),
                    conf.get("access_id"),
                    conf.get("access_key"),
                    conf.get("end_point"),
                    conf.get("dt_end_point"),
                    conf.get("logview_host"),
                    "");
    assertNotNull(rodps);
    rodps.runSqlTask("create table if not exists " + table + "(id int) comment 'This is the test table for ROdps';");
  }

  protected void tearDown() throws ROdpsException {
    assertTrue(rodps.dropTable(null, table));
  }

  private static Map<String, String> loadConfig(String file) {
    try {
      Map<String, String> ret = new HashMap<String, String>();
      FileReader fileReader = new FileReader(new File(file));
      BufferedReader br = new BufferedReader(fileReader);
      String line;
      while ((line = br.readLine()) != null) {
        if (line.startsWith("#")) {
          continue;
        }
        int idx = line.indexOf("=");
        if (idx < 0) {
          System.out.println("odps_config.ini error line:" + line);
          continue;
        }
        ret.put(line.substring(0, idx).trim(), line.substring(idx + 1, line.length()).trim());
      }
      return ret;
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  @Test
  public void testGetTablesList() throws ROdpsException, OdpsException {
    List<DataFrameItem> ret = rodps.getTables(null, null);

    for (DataFrameItem d : ret) {
      List res = d.getData();
      System.out.println(res);
    }

    assertNotNull(ret);
    assertEquals(2, ret.size());
    System.out.println("testN1GetTableList:" + ret);
  }

  @Test
  public void testIsTableExists() throws ROdpsException {
    assertTrue(rodps.isTableExist(null, table, null));
    assertFalse(rodps.isTableExist(null, table + "not_exists", null));
  }

  @Test
  public void testParsePartition() throws ROdpsException {
    assertTrue(rodps.formatPartition("pt=1/ds=2", "'", " and ").equals("pt='1' and ds='2'"));
    assertTrue(rodps.formatPartition("pt=1,ds=2", "'", " and ").equals("pt='1' and ds='2'"));
    assertTrue(rodps.formatPartition("pt='1',ds='2'", "'", " and ").equals("pt='1' and ds='2'"));
    assertTrue(rodps.formatPartition("pt=\"1\",ds=\"2\"", "'", " and ").equals(
        "pt='1' and ds='2'"));
    assertTrue(rodps.formatPartition("pt=\"1\",ds=\"2\"", "", ",").equals("pt=1,ds=2"));
    assertTrue(rodps.formatPartition("pt='1',ds='2012-01-01 00:11:11'", "", ",").equals(
        "pt=1,ds=2012-01-01 00:11:11"));
    try {
      assertTrue(rodps.formatPartition("pt='1',ds='a,b'", "", ",").equals(
          "pt=1,ds=2012-01-01 00:11:11"));
      assertTrue(false);
    } catch (Exception e) {
      assertTrue(true);
    }
    try {
      assertTrue(rodps.formatPartition("pt='1',ds='a=b'", "", ",").equals(
          "pt=1,ds=2012-01-01 00:11:11"));
      assertTrue(false);
    } catch (Exception e) {
      assertTrue(true);
    }
    try {
      assertTrue(rodps.formatPartition("pt='1',ds=',a=b'", "", ",").equals(
          "pt=1,ds=2012-01-01 00:11:11"));
      assertTrue(false);
    } catch (Exception e) {
      assertTrue(true);
    }
  }

  @Test
  public void test_DescribeTable() throws ROdpsException, OdpsException {
    List<DataFrameItem> ret = rodps.describeTable(null, table, null);
    for (DataFrameItem d : ret) {
      List res = d.getData();
      System.out.println(d.getName());
      System.out.println(res);
    }
    assertNotNull(ret);
  }

  @Test
  public void testTableSize() throws ROdpsException {
    Long ret = rodps.getTableSize(null, table, null);
    System.out.println(ret);
  }

  @Test
  public void testDtLoad() throws ROdpsException {
    String project = null;
    String partition = null;
    String colDelimiter = "\u0001";
    String rowDelimiter = "\n";
    int limit = 863;
    List<List<String>> ret =
        rodps.loadTableFromDT(project, table, partition, file, colDelimiter, rowDelimiter, limit, 8);
    System.out.println(ret);
  }

  @Test
  public void testUpload() throws ROdpsException {
    String project = null;
    String partition = null;
    String colDelimiter = "\u0001";
    String rowDelimiter = "\n";
    rodps.writeTableFromDT(project, table, partition, file, colDelimiter, rowDelimiter, 1, 8);
  }

  @Test
  public void testRunSqlTask() throws ROdpsException {
    List<String> ret = rodps.runSqlTask("create table if not exists odps_r_operator(id int);");
    assertNotNull(ret);
    assertEquals(0, ret.size());

    ret = rodps.runSqlTask("insert into table odps_r_operator select 1 from dual;");
    assertNotNull(ret);
    assertEquals(0, ret.size());
  }

}
