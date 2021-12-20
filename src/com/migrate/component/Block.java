package com.migrate.component;

/**
 * 负责保存中间数据
 * @author zmm
 * @date 2021/12/18 11:10
 */
public class Block {

    /** 库编号 */
    private final int db;
    /** 文件编号 */
    private final int index;

    private int count = 0;
    private String sql;
    private String[] data;
    private int colCount = 0;

    public Block(int db, int index, String sql) {
        this.db = db;
        this.index = index;
        this.sql = sql;
    }

    public void add(String data) {

    }

    public int getDb() {
        return db;
    }

    private int getIndex() {
        return index;
    }

    public int count() {
        return count;
    }


    public void clear() {

    }
}
