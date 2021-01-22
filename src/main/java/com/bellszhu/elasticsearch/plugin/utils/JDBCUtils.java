package com.bellszhu.elasticsearch.plugin.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class JDBCUtils {
    private static final Logger logger = LogManager.getLogger("dynamic-synonym");

    public static long queryMaxSynonymRuleVersion(String dbUrl) {
        long maxVersion = 0;
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(dbUrl);
            stmt = conn.createStatement();
            String sql = "SELECT max(modified)  modified FROM tbl_product_synonym where status = 1";
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                Timestamp date = rs.getTimestamp("modified");
                if (date == null) {
                    return System.currentTimeMillis();
                }
                logger.info("获取最后更新时间:" + date);
                return date.getTime();
            }
        } catch (SQLException | ClassNotFoundException e) {
            logger.error("获取最后更新时间出错", e);
            return System.currentTimeMillis();
        } finally {
            closeQuietly(conn, stmt, rs);
        }

        return maxVersion;
    }

    public static List<String> querySynonymRules(String dbUrl) throws Exception {
        List<String> list = new ArrayList<>();
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(dbUrl);
            stmt = conn.createStatement();
            String sql = "SELECT synonym_word FROM tbl_product_synonym WHERE status = 1";

            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                list.add(rs.getString("synonym_word"));
            }
        } finally {
            closeQuietly(conn, stmt, rs);
        }

        return list;

    }

    private static void closeQuietly(Connection conn, Statement stmt, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
            }
        }
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
            }
        }
    }
}
