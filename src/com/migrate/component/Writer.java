package com.migrate.component;

import com.migrate.util.FileUtils;
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
    private final HikariDataSource ds;

    public Writer(Block block, HikariDataSource ds) {
        this.block = block;
        this.ds = ds;
    }

    @Override
    public void run() {
        int src = block.getSrc();
        int db = block.getDb();
        int fIndex = block.getFIndex();
        long fLine = block.getFLine();
        List<String> data = block.getData();
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
            ps.clearBatch();
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (block.isEnd()) {
            fIndex++;
            fLine = 0;
        }

        // 1.正确性验证，是否全部同步成功了（是否结束了）
        if (src == 0 && db == 6 || src == 1 && db == 2) {
            // 检测是否运行结束了
            Sign.isEnd(src, fIndex);
        }
        block.clear();
        // 2.写wal文件
        FileUtils.writeWal(src, db, fIndex, fLine);
    }
}
