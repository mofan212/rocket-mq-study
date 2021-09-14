package indi.mofan.delay;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 延迟消息生产者
 *
 * @author mofan
 * @date 2021/9/14 22:41
 */
public class DelayProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        producer.setNamesrvAddr("rocketmq:9876");
        producer.start();

        for (int i = 0; i < 5; i++) {
            byte[] body = ("Hi" + i).getBytes();
            Message message = new Message("TopicB", "someTag", body);
            // 指定消息延迟等级为 3 级，即延迟 10s
            message.setDelayTimeLevel(3);
            SendResult sendResult = producer.send(message);
            // 输出消息被发送的时间
            System.out.println(new SimpleDateFormat("mm:ss").format(new Date()));
            System.out.println(" ," + sendResult);
        }
        producer.shutdown();
    }
}
