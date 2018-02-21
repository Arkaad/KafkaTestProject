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
    private static String tableName = "";
    private static String sql = "";

    public static void main(String[] args) {
        int interval = 3;//3
        long limit = 1000035; //100035

        Connection conn = null;
        PreparedStatement ps = null;
        Vector<String> dataVector = new Vector<>();
        tableName = "LINEAGE_TEST_" + interval;
        sql = "INSERT INTO " + tableName + " VALUES (?)";
        System.out.println("sql = " + sql);
        try {
            conn = DatabaseConnection.getDBConnection();
            ps = conn.prepareStatement(sql);
            long start = System.currentTimeMillis();
            for (long i = 1; i <= limit; i++) {
                dataVector.add(String.valueOf(i));
                if (dataVector.size() >= 5000) {
                    executeBatch(dataVector, ps);
                }
            }
            if (dataVector.size() > 0) {
                executeBatch(dataVector, ps);
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

    public static void executeBatch(Vector<String> dataVector, PreparedStatement ps) throws SQLException, ClassNotFoundException {
        for (String data : dataVector) {
            ps.setString(1, data);
            ps.addBatch();
        }
        int[] ints = ps.executeBatch();
        System.out.println(ints.length + " data inserted !!");
        ps.clearBatch();
        dataVector.clear();
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
