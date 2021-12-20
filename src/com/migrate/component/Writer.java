package com.migrate.component;

/**
 * 负责写入数据
 * @author zmm
 * @date 2021/12/17 21:24
 */
public class Writer implements Runnable {

    private final Block block;
    private final DataProcess dp;

    public Writer(Block block, DataProcess dataProcess) {
        this.block = block;
        this.dp = dataProcess;
    }

    @Override
    public void run() {
        dp.getDatasource(block.getDb());
    }
}
