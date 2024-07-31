package com.zbj.mq;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Description:
 * @Author: zbj
 * @Date: 2024/6/20
 */
@Slf4j
@Configuration
public class RocketMqTemplate {

    // 接入点地址，需要设置成Proxy的地址和端口列表，一般是xxx:8080;xxx:8081。
    @Value("${rocketmq.endpoint}")
    private String endpoint;

//    //生产者为啥要有topic呢,不是能往topic发信息吗
//    @Value("${rocketmq.topic}")
//    private String topic;
    private String topic="test";

    final Producer producer;

    {
        try {
            producer = ProducerSingleton.getInstance(topic);
        } catch (ClientException e) {
            throw new RuntimeException(e);
        }
    }


    public void test() {
        System.out.println(endpoint);
    }

    /**
     * 增加生产者的方法?
     * 够用原则
     *
     * @param topic
     */


    /**
     * 同步发送消息,在sendReceipt时会等待发送结果
     *
     * @param topic
     * @param content
     */
    public void syncSend(String topic, String content) {
        try {
            Message message=buildMessage(topic, content);
            final SendReceipt sendReceipt = producer.send(message);
            log.info("Send message successfully, messageId={}", sendReceipt.getMessageId());
        } catch (ClientException e) {
            log.error("Failed to send message", e);
        }
    }

    /**
     * 提取构建message
     * @param topic
     * @param content
     * @return
     */
    private Message buildMessage(String topic, String content){
        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        //获得单例的producer
        try {
            final Producer producer = ProducerSingleton.getInstance(topic);
        } catch (ClientException e) {
            throw new RuntimeException(e);
        }

        //为什么要发字节码呢,网络传输是用字节流的,这样存储可以节省空间,并且方便数据处理且和底层协议兼容
        byte[] body = content.getBytes(StandardCharsets.UTF_8);
        //tag这里先不用
        //String tag = "yourMessageTagA";
        final Message message = provider.newMessageBuilder()
                .setTopic(topic)
                // 官网建议设置,便于精准定位某条信息
                .setKeys("yourMessageKey-1c151062f96e")
                .setBody(body)
                .build();

        return message;
    }

    /**
     * 异步发送消息
     */
    public void asyncSend(String topic, String content) {
        //构建消息
        Message message = buildMessage(topic, content);

        final CompletableFuture<SendReceipt> future = producer.sendAsync(message);

        //设置回调函数,异步执行,这里最好换成自己的线程池
        ExecutorService sendCallbackExecutor = Executors.newCachedThreadPool();

        //交给第二个参数处理第一个参数的消息
        //这里蒙了,不会唤醒吗,这里得测试一下,为什么要让一个线程睡觉一个线程去异步
        future.whenCompleteAsync((sendReceipt, throwable) -> {
            if (null != throwable) {
                log.error("Failed to send message", throwable);
                // Return early.
                return;
            }
            //这里业务处理信息
            log.info("Send message successfully, messageId={}", sendReceipt.getMessageId());
        }, sendCallbackExecutor);
        // 翻译:避免主线程运行完毕???生产环境有必要吗???
        // Block to avoid exist of background threads.
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        // Close the producer when you don't need it anymore.
        // You could close it manually or add this into the JVM shutdown hook.
        // producer.close();
    }
}




