package com.migrate.component;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author zmm
 * @date 2021/12/19 20:56
 */
public class Sign {

    public static int src1EndFlag = 0;
    public static int src2EndFlag = 0;
    private static boolean flag1 = false;
    private static boolean flag2 = false;
    public static final Lock lock = new ReentrantLock();
    public static Condition condition = lock.newCondition();

    public static void isEnd(int src, int fIndex) {
        lock.lock();
        try {
            if (src == 0) {
                if (fIndex == src1EndFlag) {
                    flag1 = true;
                }
            } else {
                if (fIndex == src2EndFlag) {
                    flag2 = true;
                }
            }
            if (flag1 && flag2) {
                System.out.println("end................");
                condition.signalAll();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }
}
