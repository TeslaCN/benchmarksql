/*
 * ExecJDBC - Command line program to process SQL DDL statements, from
 *             a text input file, to any JDBC Data Source
 *
 * Copyright (C) 2004-2016, Denis Lussier
 * Copyright (C) 2016, Jan Wieck
 *
 */

import org.apache.shardingsphere.driver.api.yaml.YamlShardingSphereDataSourceFactory;
import org.apache.shardingsphere.driver.jdbc.core.connection.ShardingSphereConnection;

import java.io.*;
import java.sql.*;
import java.util.*;


public class ExecJDBC {


  public static void main(String[] args) {

    Connection conn = null;
    Statement stmt = null;
    String rLine = null;
    StringBuffer sql = new StringBuffer();

    try {

    Properties ini = new Properties();
//    System.setProperty("prop", "D:\\zhoubin\\code\\benchmarksql-5.0\\run\\props.sharding");
//    System.setProperty("commandFile", "D:\\zhoubin\\code\\benchmarksql-5.0\\run\\sql.common\\tableCreates.sql");
    ini.load( new FileInputStream(System.getProperty("prop")));

    // Register jdbcDriver
    String driverName = ini.getProperty("driver");
    Class.forName(driverName);

    // make connection
    Properties dbConnection = new Properties();
    dbConnection.setProperty("user", ini.getProperty("user"));
    dbConnection.setProperty("password", ini.getProperty("password"));
    dbConnection.setProperty("config", (String)ini.getOrDefault("config", ""));
    conn = ShardingJdbc.getConnection(ini.getProperty("conn"), dbConnection);
    conn.setAutoCommit(true);

      // Open inputFile
      BufferedReader in = new BufferedReader
        (new FileReader(jTPCCUtil.getSysProp("commandFile",null)));

      // loop thru input file and concatenate SQL statement fragments
      while((rLine = in.readLine()) != null) {

         String line = rLine.trim();

         if (line.length() != 0) {
           if (line.startsWith("--")) {
              System.out.println(line);  // print comment line
           } else {
	       if (line.endsWith("\\;"))
	       {
	         sql.append(line.replaceAll("\\\\;", ";"));
		 sql.append("\n");
	       }
	       else
	       {
		   sql.append(line.replaceAll("\\\\;", ";"));
		   if (line.endsWith(";")) {
		      String query = sql.toString();
               // Create Statement
               stmt = conn.createStatement();
		      execJDBC(stmt, query.substring(0, query.length() - 1));
		      stmt.close();
		      stmt = null;
		      sql = new StringBuffer();
		   } else {
		     sql.append("\n");
		   }
	       }
           }

         } //end if

      } //end while

      in.close();

    } catch(IOException ie) {
        System.out.println(ie.getMessage());

    } catch(SQLException se) {
        System.out.println(se.getMessage());

    } catch(Exception e) {
        e.printStackTrace();

    //exit Cleanly
    } finally {
      try {
          if (conn !=null)
              conn.close();
              conn = null;
          } catch(SQLException se) {
            se.printStackTrace();
          } // end finally

    } // end try
      System.exit(0);
  } // end main


  static void execJDBC(Statement stmt, String query) {

    System.out.println(query + ";");

    try {
      stmt.execute(query);
    }catch(SQLException se) {
      System.out.println(se.getMessage());
    } // end try

  } // end execJDBCCommand

} // end ExecJDBC Class
