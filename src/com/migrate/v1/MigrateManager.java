package com.migrate.v1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.*;

/**
 * @author zmm
 * @date 2021/12/16 18:48
 */
public class MigrateManager {

    private String walPath;

    public static String path;
    public static String ip;
    public static int port;
    public static String user;
    public static String pwd;

    private final SQL sql;
    private final Data data;

    /** 记录当前正在同步的数据库 */
    private int db;
    /** 记录当前正在同步的csv文件 */
    private int index;

    /**
     * 记录库表结构
     * a: 1.csv 1.csv 2.csv ... n.csv
     * b: 1.csv 2.csv ... n.csv
     * ...
     * g: 1.csv 3.csv ... n.csv
     */
    private final List<File>[] files = new ArrayList[7];

    /** 记录创建表的sql语句 */
    private List<String>[] tableSql;

    public MigrateManager(String path, String ip, int port, String user, String pwd) {
        this.walPath = path + "/migrate.wal";

        MigrateManager.path = path;
        MigrateManager.ip = ip;
        MigrateManager.port = port;
        MigrateManager.user = user;
        MigrateManager.pwd = pwd;

        // 初始化files
        for (int i = 0; i < 7; i++) {
            files[i] = new ArrayList<>();
        }

        sql = new SQL();
        data = new Data();
    }

    public void run(){
        // 1.连接TDSQL（创建数据库a ~ g）
        sql.createDatabase();

        // 2.文件预处理（创建表 or 加载上次保存的数据地址（后续验证数据正确性））
        // 将文件进行预处理
        loadFile();


        // TODO: 2021/12/16 3.读数据（开启记录当前读的位置并记录到文件中）
        readFile();

        // TODO: 2021/12/16 4.写TDSQL

        // TODO: 2021/12/16 5.验证数据

        // TODO: 2021/12/16 6.结束
    }

    /**
     * 加载文件
     */
    private void loadFile() {

        // 获取wal文件
        File wal = new File(walPath);

        // 是否存在wal文件（如果不存在代表是第一次启动程序）
        boolean flag = wal.exists();

        try {
            // 获取源库
            for (File source : new File(path).listFiles()) {

                if (source.getName().endsWith("wal")) {
                    continue;
                }

                // 获取源库下面的数据库
                for (File database : source.listFiles()) {
                    int dbIndex = database.getName().charAt(0) - 'a';
                    // 获取库下的sql、csv文件
                    for (File file : database.listFiles()) {
                        if (file.getName().endsWith("csv")) {
                            // 如果是csv文件
                            // 将该文件添加到库表结构中去
                            files[dbIndex].add(file);
                            continue;
                        }

                        // 如果是sql文件并且wal文件不存在
                        if (!flag) {
                            if (tableSql == null) {
                                // 如果还未初始化tableSql
                                // 进行初始化
                                tableSql = new ArrayList[7];
                                for (int x = 0; x < 7; x++) {
                                    tableSql[x] = new ArrayList<>();
                                }
                            }
                            // 添加sql语句
                            tableSql[dbIndex].add(new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8));
                        }
                    }
                }
            }

            // 对库表结构进行排序
            // 确保每次启动库表结构都是一致的。
            for (int i = 0; i < 7; i++) {
                files[i].sort((a, b) -> {
                    if (a.getName().compareTo(b.getName()) < 0) {
                        return -1;
                    }
                    return 1;
                });
            }

            if (flag) {
                // 如果wal文件存在
                // 获取wal文件内容(记录的是 库 + 下标。例如: a3)
                String s = new String(Files.readAllBytes(wal.toPath()), StandardCharsets.UTF_8);
                db = Integer.parseInt(String.valueOf(s.charAt(0)));
                index = Integer.parseInt(s.substring(1));
            } else {
                // 创建数据库表
                for (int i = 0; i < 7; i++) {
                    sql.createTable(i, tableSql[i]);
                }

                // 创建wal文件
                writeWal();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // TODO: 2021/12/17 文件切分后进行读取
    /**
     * 读取文件
     */
    private void readFile() {
        sql.changeDatabase(db);
        while (db < 7) {
            // 找到需要read的文件
            File fReading = files[db].get(index);

            String table = fReading.getName();
            int x = table.indexOf('.');
            table = table.substring(0, x);

            // 按行读取该文件内容
            try (BufferedReader br = Files.newBufferedReader(fReading.toPath())) {
                String line;
                int count = 0;
                Connection conn = sql.getConnection();

                PreparedStatement ps = conn.prepareStatement("insert ignore into `" + table + "` (id, a, b, updated_at) values (?,?,?,?)");

                while ((line = br.readLine()) != null) {
                    if (count >= 100000) {
                        ps.executeBatch();
                    }
                    String[] split = line.split(",");
                    ps.setLong(1, Long.parseLong(split[0]));
                    ps.setFloat(2, Float.parseFloat(split[1]));
                    ps.setString(3, split[2]);
                    ps.setDate(4, data.parseStringToDate(split[3]));
                    ps.addBatch();
                    count++;
                }
                if (count != 0) {
                    ps.executeBatch();
                }

                ps.close();
                conn.close();
            } catch (Exception e){
                e.printStackTrace();
            }

            // 读完后更新下标
            index++;
            int end = files[db].size();
            if (index == end) {
                index = 0;
                db++;
                sql.changeDatabase(db);
            }

            // 写wal
            writeWal();
        }
    }

    /**
     * 写wal文件
     */
    private void writeWal() {
        System.out.println("writeWal: " + db + " " + index);
        byte[] bytes = ("" + db + index).getBytes();
        try (FileOutputStream out = new FileOutputStream(new File(walPath))) {
            out.write(bytes);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
