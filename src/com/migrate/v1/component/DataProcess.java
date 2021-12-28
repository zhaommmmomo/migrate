package com.migrate.v1.component;

import com.migrate.v1.MigrateManager;
import com.migrate.util.Sign;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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

    private final int signCount;
    private final int awaitCount;

    private final Lock lock = new ReentrantLock();
    private final Condition sign = lock.newCondition();

    public DataProcess(int cap) {
        signCount = cap / 5;
        awaitCount = cap * 4 / 5;
        // 初始化数据同步器
        writerPool = new ThreadPoolExecutor(8,
                                        8,
                                           0L,
                                                        TimeUnit.MILLISECONDS,
                                                        new LinkedBlockingQueue<>(cap));

        config = new HikariConfig();
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        config.setUsername(MigrateManager.user);
        config.setPassword(MigrateManager.pwd);
        config.setMinimumIdle(4);
        config.setMaximumPoolSize(32);
        config.setAutoCommit(false);
        config.setMaxLifetime(150000);
        // 连接池参数
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        dataSources = new HikariDataSource[7];
    }

    public void addTask(Block block) {
        lock.lock();
        try {
            if (writerPool.getQueue().size() >= awaitCount) {
                sign.await();
            }
            Sign.incTask();
            writerPool.submit(new Writer(block, this));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    public HikariDataSource getDataSource(int db) {
        if (dataSources[db] == null) {
            String url = "jdbc:mysql://" + MigrateManager.ip + ":" + MigrateManager.port + "/";
            String urlEnd = "?useSSL=false&verifyServerCertificate=false";
            config.setJdbcUrl(url + ((char) (db + 'a')) + urlEnd);
            dataSources[db] = new HikariDataSource(config);
        } else {
            lock.lock();
            try {
                if (writerPool.getQueue().size() <= signCount) {
                    sign.signalAll();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
            }
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
