package indi.mofan.delay;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 延迟消息消费者
 *
 * @author mofan
 * @date 2021/9/14 22:45
 */
public class OtherConsumer {
    public static void main(String[] args) throws Exception{
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("cg");
        consumer.setNamesrvAddr("rocketmq:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        consumer.subscribe("TopicB", "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (messageList, consumeConcurrentlyContext) -> {
            for (MessageExt message : messageList) {
                // 输出消息被消费的时间
                System.out.println(new SimpleDateFormat("mm:ss").format(new Date()));
                System.out.println(" ," + message);
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        System.out.println("Consumer Started");
    }
}
