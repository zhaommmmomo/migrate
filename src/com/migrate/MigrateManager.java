package com.migrate;

import com.beust.jcommander.Parameter;
import com.migrate.component.DataProcess;
import com.migrate.component.Reader;
import com.migrate.component.Sign;
import com.migrate.component.Writer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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

    /** 读文件线程 */
    private Thread reader1;
    private Thread reader2;



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
     * 记录创建表的sql语句
     * |_a_|_b_|_c_|_d_|_e_|_f_|_g_|
     * 1.sql        ...
     * 2.sql        ...
     * 3.sql        ...
     * 4.sql        ...
     */
    private List<String>[] tableSql = new ArrayList[7];

    /** 记录每个表的insert格式 */
    private static List<String>[] insertSql = new ArrayList[7];

    /** 记录每个表的字段类型 */
    private Map<String, List<String>> types = new HashMap<>();

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
        // 初始化数据处理器
        dataProcess = new DataProcess(30);

        //cyclicBarrier = new CyclicBarrier(2, new Runnable() {
        //    @Override
        //    public void run() {
        //        // 当所有reader线程读取某一文件完成后，判断next文件是否与当前读取的文件相差1
        //
        //        // 判断每个reader的接下来一个文件是否还是当前表的数据
        //    }
        //});
    }

    /**
     * 程序启动方法
     */
    public void start() {
        // 预加载文件
        loadFile();
        // 初始化组件
        init();
        // 运行
        run();
    }

    /**
     * 程序运行方法
     */
    private void run() {
        // 启动文件读取线程
        reader1.start();
        reader2.start();

        Sign.lock.lock();
        try {
            Sign.condition.await();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            Sign.lock.unlock();
        }
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
        List<File>[] files = new ArrayList[7];

        try {
            for (File src : new File(path).listFiles()) {

                if (src.getName().endsWith("wal")) {
                    continue;
                }
                int dbIndex = 0;
                for (int i = 0; i < 7; i++) {
                    files[i] = new ArrayList<>();
                }
                for (File db : src.listFiles()) {
                    for (File file : db.listFiles()) {
                        filename = file.getName();
                        if (filename.endsWith("v")) {
                            continue;
                        }
                        if (filename.endsWith("l")) {
                            if (srcIndex == 0) {
                                tableSql[dbIndex].add(parseSqlFile(dbIndex, file));
                            }
                            continue;
                        }
                        files[dbIndex].add(file);

                    }
                    dbIndex++;
                }
                srcIndex++;
                for (int i = 0; i < 7; i++) {
                    files[i].sort((a, b) -> {
                        if (a.getName().compareTo(b.getName()) < 0) {
                            return -1;
                        }
                        return 0;
                    });
                }
                if (reader1 == null) {
                    reader1 = new Thread(new Reader(syncedDB, syncedIndex, files, dataProcess));
                } else {
                    reader2 = new Thread(new Reader(syncedDB, syncedIndex, files, dataProcess));
                }
            }

            if (flag) {
                // 如果wal文件存在
                // 获取wal文件内容(记录的是 库 + 下标。例如: 0100 ... 6401)
                String s = new String(Files.readAllBytes(wal.toPath()), StandardCharsets.UTF_8);
                readingDB = syncedDB = Integer.parseInt(String.valueOf(s.charAt(0)));
                readingIndex = syncedIndex = Integer.parseInt(s.substring(1));
            } else {
                buildDB();
                writeWal();
            }

            files = null;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 解析sql文件
     * 需要获取对应的insert sql语句
     * 类型、tableName等
     * @param db            所在的库
     * @param file          sql文件
     */
    private String parseSqlFile(int db, File file) {
        String str = null;
        StringBuilder sql = new StringBuilder("insert into ");
        char point = '`';
        String tableName = null;
        // 类型
        List<String> type = new ArrayList<>();
        try {
            str = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
            int start = str.indexOf(point);
            int end = str.indexOf(point, start + 1);

            // 字段
            List<String> col = new ArrayList<>();

            boolean key = false;
            String s = "?,";
            StringBuilder values = new StringBuilder(" values (");
            out : while (start != -1 && end != -1) {
                String var = str.substring(start, end + 1);
                if (tableName == null) {
                    tableName = var;
                    sql.append(tableName).append(" (");
                } else {
                    String t = str.substring(end + 2, end + 4);
                    switch (t) {
                        case "bi": type.add("bigint"); values.append(s); break;
                        case "fl": type.add("float"); values.append(s); break;
                        case "ch": type.add("char"); values.append(s); break;
                        case "do": type.add("double"); values.append(s); break;
                        case "in": type.add("int"); values.append(s); break;
                        case "da": type.add("datetime"); values.append(s); break;
                        default:   if (str.charAt(start - 3) == 'Y' || str.charAt(start - 2) == 'Y') {
                                        key = true;
                                   }
                                   break out;
                    }
                    col.add(var);
                    sql.append(var).append(",");
                }
                start = str.indexOf(point, end + 1);
                end = str.indexOf(point, start + 1);
            }
            sql.deleteCharAt(sql.length() - 1 )
                .append(")")
                .append(values)
                .deleteCharAt(sql.length() - 1)
                .append(") on duplicate key update ");
            int index = str.lastIndexOf(')');
            if (key) {
                for (String co : col) {
                    sql.append(co)
                            .append("=if(updated_at>=values(`updated_at`),")
                            .append(co)
                            .append(",values(")
                            .append(co)
                            .append(")),");
                }
                sql.deleteCharAt(sql.length() - 1);
                str = str.substring(0, index + 1) + " shardkey=" + col.get(0) + str.substring(index + 1);
            } else {
                sql.append("`updated_at`=if(updated_at>=values(`updated_at`), updated_at, values(`updated_at`))");

                StringBuilder sb = new StringBuilder(str.substring(0, index)).append(" ,PRIMARY KEY `uk` (");
                for (String co : col) {
                    if (co.equals("`updated_at`")) continue;
                    sb.append(co).append(",");
                }
                str = sb.deleteCharAt(sb.length() - 1)
                        .append(")\n) shardkey=")
                        .append(col.get(0))
                        .append(str.substring(index + 1))
                        .toString();
            }
            col.clear();
        } catch (Exception e) {
            e.printStackTrace();
        }

        types.put(tableName, type);
        insertSql[db].add(sql.toString());
        return str;
    }

    /**
     * 构建数据库库表
     */
    private void buildDB() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://" + ip + ":" + port + "/?useSSL=false&verifyServerCertificate=false");
        config.setDriverClassName("com.mysql.jdbc.Driver");
        config.setUsername(user);
        config.setPassword(pwd);
        // 连接池参数
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        HikariDataSource ds = new HikariDataSource(config);

        try (Connection conn = ds.getConnection();
             Statement st = conn.createStatement()) {
            char c = 'a';
            while (c <= 'g') {
                st.addBatch("create database if not exists " + c);
                c++;
            }
            for (int i = 0; i < 7; i++) {
                st.addBatch("use " + c);
                for (String table : tableSql[i]) {
                    st.addBatch(table);
                }
            }
            st.executeBatch();
        } catch (Exception e) {
            e.printStackTrace();
        }
        ds.close();
    }

    /**
     * 写wal文件
     */
    private void writeWal() {
        try (FileOutputStream out = new FileOutputStream(new File(walPath))) {
            out.write(("" + syncedDB + syncedIndex).getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取插入的sql语句
     * @param db            库
     * @param index         表下标
     * @return              sql语句
     */
    public static String getSql(int db, int index) {
        return insertSql[db].get(index);
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
