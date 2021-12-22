package com.migrate;

import com.beust.jcommander.Parameter;
import com.migrate.component.DataProcess;
import com.migrate.component.Reader;
import com.migrate.component.Sign;
import com.migrate.util.FileUtils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.Statement;
import java.util.*;

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

    /**
     * 程序启动方法
     */
    public void start() {
        dataProcess = new DataProcess(50);
        // 预加载文件
        loadFile();
        // 运行
        run();
        // 结束
        end();
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
     * 结束方法
     */
    public void end() {
        dataProcess.close();
    }

    /**
     * 加载文件目录结构
     */
    public void loadFile() {
        boolean flag = FileUtils.isExist();
        System.out.println("wal文件是否存在：" + flag);
        int srcIndex = 0;
        String filename;
        List<File>[] files;

        // reader1默认从a库开始读
        int db1 = 0;
        int fIndex1 = 0;
        long fLine1 = 0;

        // reader2默认从d库开始读
        int db2 = 3;
        int fIndex2 = 0;
        long fLine2 = 0;

        try {
            if (flag) {
                // 如果wal文件存在
                // 获取wal文件内容(记录的是 库 + 下标。例如: 0100 ... 6401)
                String s = FileUtils.readWal(0);
                db1 = Integer.parseInt(String.valueOf(s.charAt(0)));
                fIndex1 = Integer.parseInt(String.valueOf(s.charAt(1)));
                fLine1 = Long.parseLong(s.substring(2));
                s = FileUtils.readWal(1);
                db2 = Integer.parseInt(String.valueOf(s.charAt(0)));
                fIndex2 = Integer.parseInt(String.valueOf(s.charAt(1)));
                fLine2 = Long.parseLong(s.substring(2));
            }

            for (File src : new File(path).listFiles()) {

                if (src.getName().endsWith("wal")) {
                    continue;
                }
                int dbIndex = 0;
                files = new ArrayList[7];
                for (int i = 0; i < 7; i++) {
                    files[i] = new ArrayList<>();
                }
                for (File db : src.listFiles()) {
                    for (File file : db.listFiles()) {
                        filename = file.getName();
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
                    reader1 = new Thread(new Reader(0, db1, fIndex1, fLine1, files, dataProcess), "reader1");
                } else {
                    reader2 = new Thread(new Reader(1, db2, fIndex2, fLine2, files, dataProcess), "reader2");
                }
            }

            if (!flag) {
                buildDB();
                FileUtils.initWal();
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
            str = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8).replaceFirst("UNIQUE", "PRIMARY");
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
                // 如果有主键
                for (String co : col) {
                    if ("`id`".equals(co)) continue;
                    sql.append(co)
                            .append("=if(updated_at>=values(`updated_at`),")
                            .append(co)
                            .append(",values(")
                            .append(co)
                            .append(")),");
                }
                sql.deleteCharAt(sql.length() - 1);
                str = str.substring(0, index + 1) + " shardkey=id" + str.substring(index + 1);
            } else {
                // 如果没有主键
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

        insertSql[db].add(sql.toString());
        return str;
    }

    /**
     * 构建数据库库表
     */
    private void buildDB() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://" + ip + ":" + port + "/?useSSL=false&verifyServerCertificate=false");
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        config.setUsername(user);
        config.setPassword(pwd);
        config.setAutoCommit(false);
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(1);
        try (HikariDataSource ds = new HikariDataSource(config);
             Connection conn = ds.getConnection();
             Statement st = conn.createStatement()) {
            char c = 'a';
            while (c <= 'g') {
                st.addBatch("create database if not exists " + c);
                c++;
            }
            st.executeBatch();
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }

        HikariDataSource ds;
        for (int i = 0; i < 7; i++) {
            ds = dataProcess.getDataSource(i);
            try (Connection conn = ds.getConnection();
                 Statement st = conn.createStatement()) {
                for (String table : tableSql[i]) {
                    st.addBatch(table);
                }
                st.executeBatch();
                conn.commit();
                st.clearBatch();
                tableSql[i].clear();
                tableSql[i] = null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        tableSql = null;
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
