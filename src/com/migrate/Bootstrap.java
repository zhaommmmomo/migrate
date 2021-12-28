package com.migrate;

import com.beust.jcommander.JCommander;
import com.migrate.v2.MigrateManager;

/**
 * @author zmm
 * @date 2021/12/16 16:45
 */
public class Bootstrap {

    public static void main(String[] args) {
        //long time = System.currentTimeMillis();
        MigrateManager manager = new MigrateManager();
        // 1.获取传入的参数
        JCommander.newBuilder().addObject(manager).build().parse(args);
        //manager.printInput();
        // 启动
        manager.start();
        //System.out.println("time:" + (System.currentTimeMillis() - time));
    }
}
