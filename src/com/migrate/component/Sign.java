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
    private static int task = 0;

    public static void isEnd() {
        lock.lock();
        try {
            if (task == 0) {
                condition.signalAll();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    public static void incTask () {
        lock.lock();
        try {
            task++;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    public static void decTask() {
        lock.lock();
        try {
            task--;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }
}
