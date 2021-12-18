package com.migrate.v1;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 对读取到的数据进行处理
 * @author zmm
 * @date 2021/12/16 18:27
 */
public class Data {

    /** 当前正在同步的sql语句 */
    private PreparedStatement syncing;
    /** 准备同步的sql语句 */
    private final List<PreparedStatement> nextSync = new CopyOnWriteArrayList<>();
    /** 当前正在读的sql */
    private PreparedStatement readingData;

    private int line = 0;

    /**  日期解析 */
    SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * 1. 如果有主键或者非空唯一索引，唯一索引相同的情况下，
     * 以行updated_at时间戳来判断是否覆盖数据，
     * 如果updated_at比原来的数据更新，那么覆盖数据；否则忽略数据。
     * 不存在主键相同，updated_at时间戳相同，但数据不同的情况。
     *
     * 2. 如果没有主键或者非空唯一索引，如果除updated_at其他数据都一样，
     * 只更新updated_at字段；否则，插入一条新的数据。
     */
    public void addData(String data) {
        if (line >= 100000) {

        }
    }

    public Date parseStringToDate (String s) {
        try {
            return (Date) ft.parse(s);
        } catch (Exception ignored) {}
        return null;
    }
}
