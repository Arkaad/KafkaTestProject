package com.testcase.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Vector;

/**
 * Created by Arka Dutta on 16-Feb-18.
 */
public class LoadData {
    private static String ip = "192.168.33.187";
    private static String port = "1530";
    private static String database = "PRODUCT_HUB";
    private static String username = "PRODUCT_HUB";
    private static String password = "PRODUCT_HUB";
    private static String tableName = "LOAD_TEST_187";
    private static long startLimit = 2000001;
    private static long endLimit = 4000000;

    public static void main(String[] args) {

        Connection conn = null;
        PreparedStatement ps = null;
        String sql = "INSERT INTO " + tableName + " VALUES (?)";
        System.out.println("sql = " + sql);
        try {
            conn = DatabaseConnection.getDBConnection(ip, port, database, username, password);
            ps = conn.prepareStatement(sql);
            long start = System.currentTimeMillis();
            long total = 0L;
            long count = 0L;
            for (long i = startLimit; i <= endLimit; i++) {
                ps.setLong(1, i);
                ps.addBatch();
                count++;
                if (count >= 5000) {
                    total += ps.executeBatch().length;
                    ps.clearBatch();
                    count = 0;
                    System.out.println("Total Inserted : " + total);
                }
            }
            if (count != 0) {
                total += ps.executeBatch().length;
                ps.clearBatch();
                System.out.println("Total Inserted : " + total);
            }
            System.out.println("Time taken : " + (System.currentTimeMillis() - start) + " ms.");
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void readData() throws SQLException {
        String sql = "";
        PreparedStatement ps = null;
        Connection conn = null;
        ResultSet rs = null;
        sql = "SELECT TEST_ID FROM LINEAGE_TEST_" + 0;
        System.out.println("sql = " + sql);
        try {
            conn = DatabaseConnection.getDBConnection();
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getString(1));
                break;
            }
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (ps != null) {
                ps.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }
}
