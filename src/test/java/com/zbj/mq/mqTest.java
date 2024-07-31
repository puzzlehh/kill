package com.zbj.mq;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.apis.*;
import org.apache.rocketmq.client.apis.consumer.*;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.ProducerBuilder;
import org.apache.rocketmq.client.apis.producer.SendReceipt;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.stereotype.Component;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

/**
 * @Description:
 * @Author: zbj
 * @Date: 2024/6/15
 */
@SpringBootTest
@Slf4j
public class mqTest {

    @Autowired
    public RocketMqTemplate rocketMqTemplate;

    @Test
    public void testValue(){
        rocketMqTemplate.test();
    }

    @Test
    public void produce() throws ClientException {
        // 接入点地址，需要设置成Proxy的地址和端口列表，一般是xxx:8080;xxx:8081。
        String endpoint = "10.21.32.237:8080";
        // 消息发送的目标Topic名称，需要提前创建。
        String topic = "test";
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        ClientConfigurationBuilder builder = ClientConfiguration.newBuilder().
                setEndpoints(endpoint).
                enableSsl(true);
        ClientConfiguration configuration = builder.build();
        // 初始化Producer时需要设置通信配置以及预绑定的Topic。
        Producer producer = provider.newProducerBuilder()
                .setTopics(topic)
                .setClientConfiguration(configuration)
                .build();
        try {
            for (int i = 0; i < 10; i++) {
                // 发送消息，需要关注发送结果，并捕获失败等异常。
                org.apache.rocketmq.client.apis.message.Message message = provider.newMessageBuilder()
                        .setTopic(topic)
                        // 设置消息索引键，可根据关键字精确查找某条消息。
                        .setKeys("messageKey")
                        // 设置消息Tag，用于消费端根据指定Tag过滤消息。
                        .setTag("messageTag")
                        // 消息体。为啥要getBytes
                        .setBody(("messageBody" + "-" + i).getBytes())
                        .build();
                SendReceipt sendReceipt = producer.send(message);
                log.info("Send message successfully, messageId={}", sendReceipt.getMessageId());
            }
        } catch (ClientException e) {
            log.error("Failed to send message", e);
        }
        //自动关闭了?还是单纯搜索不到producer,无身份
        // producer.close();
    }

    static int cnt=0;

    @Test
    public void testFinal(){
        //为啥要单独建一个这个来provider呢
        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        cnt++;
        if(cnt>=10)return;
        System.out.println(provider);
        testFinal();
    }

    @Test
    public void testConsumer(){
        //为啥要单独建一个这个来provider呢
        ClientServiceProvider provider = ClientServiceProvider.loadService();

        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
                .setEndpoints("10.24.3.166:8080;10.24.3.167:8080")
                .setRequestTimeout(Duration.ofMinutes(10))
                .enableSsl(false)
                .build();

        PushConsumerBuilder builder = provider.newPushConsumerBuilder()
                .setConsumerGroup("YourConsumerGroup")
                .setClientConfiguration(clientConfiguration)
                .setConsumptionThreadCount(1)
                .setMaxCacheMessageCount(100)
                .setMessageListener((message) -> {
                    System.out.printf("Received message: %s %n", message);
                    return ConsumeResult.SUCCESS;
                });



        // 设置消费者组，需要提前创建。

    }

    @Test
    public void testProvider() {
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        ProducerBuilder producerBuilder = provider.newProducerBuilder();


//        SessionCredentialsProvider credentialsProvider = SessionCredentialsProvider.newBuilder()

        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
                .setEndpoints("10.24.3.166:8080;10.24.3.167:8080")
                .setRequestTimeout(Duration.ofMinutes(10))
                .enableSsl(false)
                .build();

        //配置属性
//        producerBuilder
//                .setTopics("TestTopic", "TestTopic2")
//                .setMaxAttempts(3)
//                .setClientConfiguration()
    }

    @Test
    public void consume() throws ClientException, IOException, InterruptedException {
        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        // 接入点地址，需要设置成Proxy的地址和端口列表，一般是xxx:8081;xxx:8081。
        String endpoints = "10.24.3.166:8080;10.24.3.167:8080";
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
                .setEndpoints(endpoints)
                .build();
        // 订阅消息的过滤规则，表示订阅所有Tag的消息。
        String tag = "*";
        FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);
        // 为消费者指定所属的消费者分组，Group需要提前创建。
        String consumerGroup = "YourConsumerGroup";
        // 指定需要订阅哪个目标Topic，Topic需要提前创建。
        String topic = "TestTopic";
        // 初始化PushConsumer，需要绑定消费者分组ConsumerGroup、通信参数以及订阅关系。
        PushConsumer pushConsumer = provider.newPushConsumerBuilder()
                .setClientConfiguration(clientConfiguration)
                // 设置消费者分组。
                .setConsumerGroup(consumerGroup)
                // 设置预绑定的订阅关系。
                .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
                // 设置消费监听器。
                .setMessageListener(messageView -> {
                    // 处理消息并返回消费结果。
                    log.info("Consume message successfully, messageId={}", messageView.getMessageId());
                    return ConsumeResult.SUCCESS;
                })
                .build();
        Thread.sleep(Long.MAX_VALUE);
        // 如果不需要再使用 PushConsumer，可关闭该实例。
        // pushConsumer.close();
    }

}

