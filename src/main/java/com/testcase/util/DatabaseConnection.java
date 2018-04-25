package com.testcase.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by Arka Dutta on 16-Feb-18.
 */
public class DatabaseConnection {
    private static String driver = "org.apache.derby.jdbc.ClientDriver";
    private static String ip = "192.168.33.218";
    private static String port = "1530";
    private static String database = "CRM";
    private static String username = "CRM";
    private static String password = "CRM";

    public static Connection getDBConnection() throws ClassNotFoundException, SQLException {
        return getDBConnection(ip, port, database, username, password);
    }

    public static Connection getDBConnection(String ip, String port, String database, String username, String password) throws ClassNotFoundException, SQLException {
        try {
            Class.forName(driver);
            String url = "jdbc:derby://" + ip + ":" + port + "/" + database + ";ssl=basic";
            return DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
            throw e;
        }
    }
}
