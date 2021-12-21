package com.migrate.component;

import com.migrate.MigrateManager;

import java.io.BufferedReader;
import java.io.File;
import java.nio.file.Files;
import java.util.List;

/**
 * 负责加载数据
 * @author zmm
 * @date 2021/12/17 21:23
 */
public class Reader implements Runnable{

    /** 源库 */
    private final int src;
    /** 数据库编号 */
    private int db;
    /** 文件编号 */
    private int fIndex;
    /** 读取文件的起始行 */
    private final long startLine;

    private int err = 0;

    private final int end;

    private final DataProcess dp;

    /** 需要读的文件 */
    private final List<File>[] files;

    public Reader(int src, int db, int fIndex, long startLine, List<File>[] files, DataProcess dp) {
        this.src = src;
        if (src == 0) {
            this.db = db;
            end = 7;
        } else {
            this.db = db <= 2 ? db + 7 : db;
            end = 10;
        }
        if (files[db].size() == fIndex) {
            this.fIndex = 0;
            this.db++;
        } else {
            this.fIndex = fIndex;
        }
        this.files = files;
        this.dp = dp;
        this.startLine = startLine;
    }

    @Override
    public void run() {
        Block block;
        String sql;
        String line;
        long fLine;
        while (db < end) {
            fLine = 0L;
            // 获取当前读取的文件的库
            int i = src == 0 ? db : db % 7;

            // 获取该文件
            File fReading = files[i].get(fIndex);
            sql = MigrateManager.getSql(i, Integer.parseInt(String.valueOf(fReading.getName().charAt(0))) - 1);
            block = new Block(src, i, fIndex, sql);
            try (BufferedReader br = Files.newBufferedReader(fReading.toPath())) {
                int count = 0;
                while ((line = br.readLine()) != null) {
                    fLine++;
                    if (fLine <= startLine) continue;
                    if (count == 50000) {
                        block.setFLine(fLine);
                        dp.addTask(block);
                        block = new Block(src, i, fIndex, sql);
                        count = 0;
                    }
                    block.add(line);
                    count++;
                }
            } catch (Exception e) {
                e.printStackTrace();
                if (++err < 3) {
                    this.run();
                }
            }
            if (block.size() != 0) {
                block.setEnd();
                block.setFLine(fLine);
                dp.addTask(block);
            }
            fIndex++;
            err = 0;
            if (fIndex == files[i].size()) {
                db++;
                fIndex = 0;
            }
        }
    }
}
