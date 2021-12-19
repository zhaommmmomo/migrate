package com.migrate;

import com.beust.jcommander.Parameter;
import com.migrate.component.DataProcess;
import com.migrate.component.Reader;
import com.migrate.component.Writer;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author zmm
 * @date 2021/12/18 11:15
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

    /** 数据处理器 */
    private DataProcess dataProcess;

    /** 文件读取池(根据多少个数据源来定义线程数) */
    ExecutorService readerPool;

    /** 数据同步池 */
    ExecutorService writerPool;

    /** 记录当前正在读的文件 */
    private int readingIndex = 0;
    /** 记录当前正在读的库 */
    private int readingDB = 0;

    /** 记录已经同步了的文件 */
    private int syncedIndex = 0;
    /** 记录已经同步了的库 */
    private int syncedDB = 0;

    private String walPath;

    /** 每组文件读取 */
    private CyclicBarrier cyclicBarrier;

    /**
     * 记录目录结构
     * |------------src1-----------|----------- src2-----------|
     * |_a_|_b_|_c_|_d_|_e_|_f_|_g_|_a_|_b_|_c_|_d_|_e_|_f_|_g_|
     * 100                        ...                       100
     * 101                        ...                       101
     *                            ...
     * 4xx                        ...                       4xx
     */
    private final List<File>[] files = new ArrayList[14];

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
    private List<String>[] insertSql = new ArrayList[7];

    /** 源数据库个数 */
    private int srcCount;
    private int currentFileNums;

    public MigrateManager() {
        for (int i = 0; i < 7; i++) {
            tableSql[i] = new ArrayList<>();
            insertSql[i] = new ArrayList<>();
        }
    }

    /**
     * 程序初始化方法
     */
    private void init() {
        // 初始化文件读取器

        // 初始化数据处理器

        // 初始化数据同步器

        cyclicBarrier = new CyclicBarrier(srcCount, new Runnable() {
            @Override
            public void run() {
                // 当所有reader线程读取某一文件完成后，判断next文件是否与当前读取的文件相差1

                // 判断每个reader的接下来一个文件是否还是当前表的数据
            }
        });
    }


    /**
     * 程序启动方法
     */
    public void run () {
        // 预加载文件
        loadFile();

        init();


        dataProcess.marge();
    }

    /**
     * 加载文件目录结构
     */
    public void loadFile() {
        walPath = path + "/migrate.wal";
        File wal = new File(walPath);
        boolean flag = wal.exists();
        int srcIndex = 0;
        String filename;
        try {
            for (File src : new File(path).listFiles()) {

                if (src.getName().endsWith("wal")) {
                    continue;
                }
                int dbIndex = 0;
                for (File db : src.listFiles()) {
                    for (File file : db.listFiles()) {
                        filename = file.getName();
                        if (filename.endsWith("v")) {
                            continue;
                        }
                        if (filename.endsWith("l")) {
                            tableSql[dbIndex].add(parseSqlFile(file));
                            continue;
                        }
                        files[srcIndex].add(file);
                    }
                    dbIndex++;
                    srcIndex++;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 解析sql文件
     * 需要获取对应的insert sql语句
     * 类型、字段、key
     * @param file          sql文件
     */
    private String parseSqlFile(File file) {

        return null;
    }

    /**
     * 打印参数信息
     */
    public void printInput() {
        System.out.printf("data path:%s\n",path);
        System.out.printf("dst ip:%s\n",ip);
        System.out.printf("dst port:%d\n",port);
        System.out.printf("dst user:%s\n",user);
        System.out.printf("dst password:%s\n",pwd);
    }
}
