package com.migrate.component;

import com.migrate.MigrateManager;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 负责处理数据
 * @author zmm
 * @date 2021/12/18 11:13
 */
public class DataProcess {

    /** 数据同步池 */
    private final ThreadPoolExecutor writerPool;

    /** 数据源 */
    private final HikariDataSource[] dataSources;

    /** 配置 */
    private final HikariConfig config;

    public DataProcess(int cap) {
        // 初始化数据同步器
        writerPool = new ThreadPoolExecutor(8,
                                        64,
                                           0L,
                                                        TimeUnit.MILLISECONDS,
                                                        new LinkedBlockingQueue<>(cap));

        config = new HikariConfig();
        config.setDriverClassName("com.mysql.jdbc.Driver");
        config.setUsername(MigrateManager.user);
        config.setPassword(MigrateManager.pwd);
        config.setMinimumIdle(4);
        config.setMaximumPoolSize(64);
        config.setAutoCommit(false);
        config.setMaxLifetime(60000);
        // 连接池参数
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        dataSources = new HikariDataSource[7];
    }

    public void addTask(Block block) {
        writerPool.submit(new Writer(block, getDataSource(block.getDb())));
    }

    public HikariDataSource getDataSource(int db) {
        if (dataSources[db] == null) {
            String url = "jdbc:mysql://" + MigrateManager.ip + ":" + MigrateManager.port + "/";
            String urlEnd = "?useSSL=false&verifyServerCertificate=false";
            config.setJdbcUrl(url + ((char) (db + 'a')) + urlEnd);
            dataSources[db] = new HikariDataSource(config);
        }
        return dataSources[db];
    }

    public void close() {
        writerPool.shutdown();
        for (int i = 0; i < 7; i++) {
            if (dataSources[i] != null && dataSources[i].isRunning()) {
                dataSources[i].close();
            }
        }
    }
}
