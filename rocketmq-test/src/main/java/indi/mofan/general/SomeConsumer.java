package indi.mofan.general;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 消息消费者
 *
 * @author mofan
 * @date 2021/9/12 22:07
 */
public class SomeConsumer {
    public static void main(String[] args) throws Exception{
        // 定义一个 pull 消费者
        /* DefaultLitePullConsumer consumer = new DefaultLitePullConsumer("cg"); */
        // 定义一个 push 消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("cg");
        // 指定 NameServer
        consumer.setNamesrvAddr("rocketmq:9876");
        // 指定从第一条消息开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // 指定消费 Topic 和 Tag
        consumer.subscribe("someTopic", "");
        // 指定采用“广播模式”进行消费，默认为集群模式
        /* consumer.setMessageModel(MessageModel.BROADCASTING); */
        // 注册消息监听器
        consumer.registerMessageListener((MessageListenerConcurrently) (messageList, consumeConcurrentlyContext) -> {
            // 一旦 Broker 中有了其订阅消息就会触发该 Lambda 表达式的执行
            // 其返回值为当前 consumer 消费的状态

            // 逐条发送消息
            for (MessageExt msg : messageList) {
                System.out.println(msg);
            }
            // 返回消费状态，消费成功
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        // 开启消费者消费
        consumer.start();
        System.out.println("Consumer Started");
    }
}
