package com.migrate;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author zmm
 * @date 2021/12/18 16:06
 */
public class Test {

    public static void main(String[] args) {
        int[] times = new int[]{11111,22222,33333,44444,55555,66666,77777,88888,99999,12345};
        Map<Long, Integer> map = new HashMap<>();

        for (long i = 10000000; i < 11000000L; i += 10) {
            map.put(i, 0);
            map.put(i + 1, 1);
            map.put(i + 2, 2);
            map.put(i + 3, 3);
            map.put(i + 4, 4);
            map.put(i + 5, 5);
            map.put(i + 6, 6);
            map.put(i + 7, 7);
            map.put(i + 8, 8);
            map.put(i + 9, 9);
        }

        try {
            TimeUnit.SECONDS.sleep(30);
        } catch (Exception ignored){}
    }
}
