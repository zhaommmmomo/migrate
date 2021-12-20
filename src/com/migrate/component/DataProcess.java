package com.migrate.component;

import com.migrate.MigrateManager;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.util.concurrent.ExecutorService;
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
    private final ExecutorService writerPool;

    /** 数据源 */
    private final HikariDataSource[] dataSources;

    public DataProcess(int cap) {
        // 初始化数据同步器
        writerPool = new ThreadPoolExecutor(8,
                                        16,
                                           0L,
                                                        TimeUnit.MILLISECONDS,
                                                        new LinkedBlockingQueue<>(cap));

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://" + MigrateManager.ip + ":" + MigrateManager.port + "/?useSSL=false&verifyServerCertificate=false");
        config.setDriverClassName("com.mysql.jdbc.Driver");
        config.setUsername(MigrateManager.user);
        config.setPassword(MigrateManager.pwd);
        // 连接池参数
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        dataSources = new HikariDataSource[7];

        String url = "jdbc:mysql://" + MigrateManager.ip + ":" + MigrateManager.port + "/";
        String urlEnd = "?useSSL=false&verifyServerCertificate=false";
        char c = 'a';
        for (int i = 0; i < 7; i++) {
            config.setJdbcUrl(url + c + urlEnd);
            dataSources[i] = new HikariDataSource(config);
            c++;
        }
    }

    public void addTask(Block block) {
        writerPool.submit(new Writer(block, this));
    }

    public HikariDataSource getDatasource(int index) {
        return dataSources[index];
    }
}
