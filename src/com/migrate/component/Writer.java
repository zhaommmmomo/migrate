package com.migrate.component;


import com.migrate.util.FileUtils;
import com.migrate.util.Sign;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * 负责写入数据
 * @author zmm
 * @date 2021/12/17 21:24
 */
public class Writer implements Runnable {
    private final Block block;
    private final DataProcess dp;

    public Writer(Block block, DataProcess dp) {
        this.block = block;
        this.dp = dp;
    }

    @Override
    public void run() {
        int src = block.getSrc();
        int db = block.getDb();
        int fIndex = block.getFIndex();
        long fLine = block.getFLine();
        List<String> data = block.getData();
        HikariDataSource ds = dp.getDataSource(db);
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(block.getSql())) {
            int i;
            for (String str : data) {
                String[] cols = str.split(",");
                i = 1;
                for (String col : cols) {
                    ps.setObject(i, col);
                    i++;
                }
                ps.addBatch();
            }
            ps.executeBatch();
            conn.commit();
            ps.clearBatch();
        } catch (Exception e) {
            e.printStackTrace();
        }
        Sign.decTask();
        if (block.isEnd()) {
            fIndex++;
            fLine = 0;
        }
        // 1.正确性验证，是否全部同步成功了（是否结束了）
        if (db == 6 && src == 0 || db == 2 && src == 1) {
            // 检测是否运行结束了
            Sign.isEnd();
        }
        block.clear();
        // 2.写wal文件
        FileUtils.writeWal(src, db, fIndex, fLine);
    }
}
