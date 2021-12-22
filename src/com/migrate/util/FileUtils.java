package com.migrate.util;

import com.migrate.MigrateManager;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

/**
 * @author zmm
 * @date 2021/12/20 15:11
 */
public class FileUtils {

    private static final String path = System.getProperty("user.dir");

    /**
     * 初始化wal文件
     */
    public static void initWal() {
        writeWal(0,0,0, 0);
        writeWal(1,3, 0, 0);
    }

    public static boolean isExist() {
        return new File(path + "/0.wal").exists();
    }

    /**
     * 写wal文件
     * @param src           源库
     * @param db            数据库
     * @param fIndex        文件下标
     * @param fLine         已同步的行下标
     */
    public static void writeWal(int src , int db, int fIndex, long fLine) {
        try (FileOutputStream out = new FileOutputStream(
                new File(path + "/" + src + ".wal"))) {
            out.write(("" + db + fIndex + fLine).getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 读wal文件
     * @param src       源库
     * @return          wal内容
     */
    public static String readWal(int src) {
        try {
            return new String(Files.readAllBytes(
                    new File(path + "/" + src + ".wal").toPath()),
                                        StandardCharsets.UTF_8);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "000";
    }
}
