package com.migrate.component;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author zmm
 * @date 2021/12/19 20:56
 */
public class Sign {
    public static final Lock lock = new ReentrantLock();
    public static Condition condition = lock.newCondition();
}
