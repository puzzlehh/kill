package com.zbj.mq;

import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.junit.Test;

/**
 * @Description:
 * @Author: zbj
 * @Date: 2024/6/21
 */
public class TestFinal {
    public static void main(String[] args) {
        testFinal();
    }

    private static int cnt = 0;

    public static void testFinal(){
        //为啥要单独建一个这个来provider呢
        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        cnt++;
        if(cnt>=10)return;
        System.out.println(provider);
        testFinal();
    }
}
