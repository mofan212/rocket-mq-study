package indi.mofan.transaction;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 工行事务监听器
 *
 * @author mofan
 * @date 2021/9/15 23:29
 */
public class ICBCTransactionListener implements TransactionListener {

    /**
     * 回调操作方法
     * 消息预提交成功就会触发该方法的执行，用于完成本地事务
     *
     * @param message 消息
     * @param o
     * @return
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object o) {
        System.out.println("预提交消息成功：" + message);
        /* 假设接收到的 TAGA 的消息就表示扣款操作成功，TAGB 的消息表示扣款失败，
        *  TAGC 表示扣款结果不清楚，需要执行消息回查*/
        if (StringUtils.equals("TAGA", message.getTags())) {
            return LocalTransactionState.COMMIT_MESSAGE;
        } else if (StringUtils.equals("TAGB", message.getTags())) {
            return LocalTransactionState.ROLLBACK_MESSAGE;
        } else if (StringUtils.equals("TAGC", message.getTags())) {
            return LocalTransactionState.UNKNOW;
        }
        return LocalTransactionState.UNKNOW;
    }

    /**
     * 消息回查方法，常见的引发原因有：
     * 1、回调操作返回 UNKNOWN
     * 2、TC 没有接收到 TM 的最终全局事务确认指令
     *
     * @param messageExt
     * @return
     */
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
        System.out.println("执行消息回查" + messageExt.getTags());
        return LocalTransactionState.COMMIT_MESSAGE;
    }
}
