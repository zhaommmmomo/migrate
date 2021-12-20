package com.migrate;

import com.migrate.component.Sign;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author zmm
 * @date 2021/12/18 16:06
 */
public class Test {

    public static void main(String[] args) {

        new Thread(() -> {
            Sign.lock.lock();
            try {
                TimeUnit.SECONDS.sleep(5);
                System.out.println("start");
                Sign.condition.signalAll();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                Sign.lock.unlock();
            }
        }).start();

        Sign.lock.lock();
        try {
            Sign.condition.await();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            Sign.lock.unlock();
        }
        System.out.println("over");
    }
}
