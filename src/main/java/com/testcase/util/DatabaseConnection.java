package com.testcase.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by Arka Dutta on 16-Feb-18.
 */
public class DatabaseConnection {
    private static String driver = "org.apache.derby.jdbc.ClientDriver";
    private static String URL = "jdbc:derby://192.168.33.218:1530/CRM;ssl=basic";
    private static String username = "CRM";
    private static String password = "CRM";

    public static Connection getDBConnection() throws ClassNotFoundException, SQLException {
        try {
            Class.forName(driver);
            return DriverManager.getConnection(URL, username, password);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
            throw e;
        }
    }
}
