package com.migrate.v1;

import com.migrate.v1.MigrateManager;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

/**
 * @author zmm
 * @date 2021/12/16 15:00
 */
public class SQL {

    public final char[] dbs = new char[]{'a', 'b', 'c', 'd', 'e', 'f', 'g'};

    private HikariDataSource ds;
    private final HikariConfig config;

    public static final String syncSql = "insert ignore into ? (id, a, b, updated_at) values (?,?,?,?)";

    public SQL() {
        // 创建数据库 a ~ g
        config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://" + MigrateManager.ip + ":" + MigrateManager.port + "/?useSSL=false&verifyServerCertificate=false");
        config.setDriverClassName("com.mysql.jdbc.Driver");
        config.setUsername(MigrateManager.user);
        config.setPassword(MigrateManager.pwd);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        ds = new HikariDataSource(config);
    }

    /**
     * 创建数据库 a ~ g
     */
    public void createDatabase() {
        try (Connection conn = ds.getConnection()) {
            char c = 'a';
            Statement st = conn.createStatement();
            String sql = "create database if not exists ";
            while (c <= 'g') {
                st.addBatch(sql + c);
                c++;
            }
            st.executeBatch();
            st.closeOnCompletion();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 在指定数据库中创建多个表
     * @param db            下标
     * @param sqls          sql语句链表
     */
    public void createTable(int db, List<String> sqls) {
        changeDatabase(db);
        try (Connection conn = ds.getConnection()) {
            Statement st = conn.createStatement();
            for (String sql : sqls) {
                st.addBatch(sql);
            }
            st.executeBatch();
            st.closeOnCompletion();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void changeDatabase(int db) {
        config.setJdbcUrl("jdbc:mysql://" + MigrateManager.ip + ":" + MigrateManager.port + "/" + dbs[db] + "?useSSL=false&verifyServerCertificate=false");
        if (ds != null) {
            ds.close();
        }
        ds = new HikariDataSource(config);
    }

    public Connection getConnection() {
        try {
            return ds.getConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
