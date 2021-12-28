package com.migrate.v1.component;

import java.util.ArrayList;
import java.util.List;

/**
 * 负责保存中间数据
 * @author zmm
 * @date 2021/12/18 11:10
 */
public class Block {

    /** 源库 */
    private final int src;
    /** 库编号 */
    private final int db;
    /** 文件编号 */
    private final int fIndex;
    /** 最后一行在文件中的行数 */
    private long fLine;
    /** 当前block的最后一行是否是文件的最后一行 */
    private boolean end;

    private final String sql;
    private List<String> data;

    public Block(int src, int db, int fIndex, String sql) {
        this.src = src;
        this.db = db;
        this.fIndex = fIndex;
        this.sql = sql;
        data = new ArrayList<>();
    }

    public void add(String data) {
        this.data.add(data);
    }

    public int getSrc() {
        return src;
    }

    public int getDb() {
        return db;
    }

    public int getFIndex() {
        return fIndex;
    }

    public void setEnd() {
        end = true;
    }

    public void setFLine(long fLine) {
        this.fLine = fLine;
    }

    public long getFLine() {
        return this.fLine;
    }

    public boolean isEnd() {
        return end;
    }

    public String getSql() {
        return sql;
    }

    public List<String> getData() {
        return data;
    }

    public int size() {
        return data.size();
    }

    public void clear() {
        data.clear();
        data = null;
    }
}
