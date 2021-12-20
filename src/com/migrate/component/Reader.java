package com.migrate.component;

import com.migrate.MigrateManager;

import java.io.BufferedReader;
import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * 负责加载数据
 * @author zmm
 * @date 2021/12/17 21:23
 */
public class Reader implements Runnable{

    /** 数据库编号 */
    private int db;
    /** 文件编号 */
    private int index;

    private int err = 0;

    private DataProcess dp;

    /** 需要读的文件 */
    private List<File>[] files;

    public Reader(int db, int index, List<File>[] files, DataProcess dp) {
        this.db = db;
        this.index = index;
        this.files = files;
        this.dp = dp;
    }

    @Override
    public void run() {
        Block block;
        String sql;
        String line;
        while (db < 7) {
            File fReading = files[db].get(index);
            sql = MigrateManager.getSql(db, Integer.parseInt(String.valueOf(fReading.getName().charAt(0))) - 1);
            block = new Block(db, index, sql);
            try (BufferedReader br = Files.newBufferedReader(fReading.toPath())) {
                while ((line = br.readLine()) != null) {
                    block.add(line);
                }
            } catch (Exception e) {
                e.printStackTrace();
                if (++err < 3) {
                    this.run();
                }
            }
            dp.addTask(block);
            index++;
            err = 0;
            if (index == files[db].size()) {
                db++;
                index = 0;
            }
        }

    }
}
