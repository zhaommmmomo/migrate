package com.migrate;

import com.beust.jcommander.Parameter;
import com.migrate.component.DataProcess;
import com.migrate.component.Reader;
import com.migrate.component.Writer;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
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
    public static String DataPath = "";

    @Parameter(names = {"--dst_ip"})
    public static String DstIP = "";

    @Parameter(names = {"--dst_port"})
    public static Integer DstPort = 0;

    @Parameter(names = {"--dst_user"})
    public static String DstUser = "";

    @Parameter(names = {"--dst_password"})
    public static String DstPassword = "";

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
     * list  []   list
     * src_1 a    100 101 ...
     *       ...
     *       g
     * src_2 a    100 101 ...
     *       ...
     *       g
     */
    private List<List<File>[]> files = new ArrayList<>();

    /** 源数据库个数 */
    private int srcCount;
    private int currentFileNums;

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



                //for (int i = 0; i < srcCount; i++) {
                //    files.get(i)[readingDB].get()
                //}
                //
                //int currentFileName = Integer.parseInt(files.get());
                //
                //
                //readingIndex++;
                //if (readingIndex >= currentFileNums) {
                //    readingDB++;
                //    readingIndex = 0;
                //}

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

    }

    /**
     * 打印参数信息
     */
    public void printInput() {
        System.out.printf("data path:%s\n",DataPath);
        System.out.printf("dst ip:%s\n",DstIP);
        System.out.printf("dst port:%d\n",DstPort);
        System.out.printf("dst user:%s\n",DstUser);
        System.out.printf("dst password:%s\n",DstPassword);
    }
}
