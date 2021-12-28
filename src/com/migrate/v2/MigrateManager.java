package com.migrate.v2;

import com.beust.jcommander.Parameter;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zmm
 * @date 2021/12/26 8:46
 */
public class MigrateManager {

    @Parameter(names = {"--data_path"})
    public static String path = "";

    @Parameter(names = {"--dst_ip"})
    public static String ip = "";

    @Parameter(names = {"--dst_port"})
    public static Integer port = 0;

    @Parameter(names = {"--dst_user"})
    public static String user = "";

    @Parameter(names = {"--dst_password"})
    public static String pwd = "";

    /**
     * 记录创建表的sql语句
     * |_a_|_b_|_c_|_d_|_e_|_f_|_g_|
     * 1.sql        ...
     * 2.sql        ...
     * 3.sql        ...
     * 4.sql        ...
     */
    private List<String>[] tableSql = new ArrayList[7];

    /** 记录每个表的insert格式 */
    private static final List<String>[] insertSql = new ArrayList[7];

    public MigrateManager() {
        for (int i = 0; i < 7; i++) {
            tableSql[i] = new ArrayList<>();
            insertSql[i] = new ArrayList<>();
        }
    }

    public void start() {

    }

    private void buildDB() {

    }
}
