package com.liyc.mqs.mqserver.datacenter;

import com.liyc.mqs.mqserver.core.Message;
import com.liyc.mqs.mqserver.core.Queue;
import com.liyc.mqs.mqserver.tool.BinaryTool;

import java.io.*;
import java.util.LinkedList;
import java.util.Scanner;

/**
 * 数据文件存储工厂类
 *  - stat类 + 读写stat（InputStream-Scanner OutputStreadm-PrintWriter）
 *  - stat的sumMsg get set方法
 *  - 获取数据目录方法；创建队列数据目录文件方法，并初始化stat文件数据 0 0。
 *  - 删除文件目录方法：删除队列目录文件
 *  - 新增Message，把消息写入队列中的方法：①拿到队列目录 ②上锁 ③序列化 ④message初始位置和起始位置 ⑤写入
 *  - 删除message：①上锁 ②找到队列 ③randomaccessfile 读取文件 ④设置无效 ⑤写回文件
 *  - message初始化到内存:
 *  -
 *
 * @author Liyc
 * @date 2024/12/13 15:45
 **/

public class MessageFileManager {
    //先创建一下个stat类来作为消息数量属性，如果设置全局属性会有并发问题，设置一个内部类挺好
    static public class Stat{
        public int sumMsg;
        public int countMsg;
    }
    //初始化，如果后续有扩展可以加
    public void init() {

    }

    //获取队列路径
    public String queuePath(String queueName) {
        return "./data/" + queueName;
    }
    //获取消息数据文件路径
    public String msgDataPath(String queueName) {
        return queuePath(queueName) + "/queue_data.txt";
    }
    //获取消息统计文件
    public String msgCountPath(String queueName) {
        return queuePath(queueName) + "/queue_stat.txt";
    }

    //把消息统计信息写入文件
    public void writeMsgCount(String queueName, Stat stat) {
        // 使用 PrintWrite 来写文件.
        // OutputStream 打开文件, 默认情况下, 会直接把原文件清空. 此时相当于新的数据覆盖了旧的.
        try {
            OutputStream file = new FileOutputStream(msgCountPath(queueName));
            String msgCount = stat.sumMsg + "\t" + stat.countMsg;
            PrintWriter printWriter = new PrintWriter(file);
            printWriter.write(msgCount);
            printWriter.flush();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
    //读取消息统计信息文件
    public Stat readMsgCount(String queueName) {
        // 由于当前的消息统计文件是文本文件, 可以直接使用 Scanner 来读取文件内容
        Stat stat = new Stat();
        try {
            InputStream file = new FileInputStream(msgCountPath(queueName));
            Scanner scanner = new Scanner(file);
            stat.sumMsg = scanner.nextInt();
            stat.countMsg = scanner.nextInt();
            return stat;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }
    //创建目录
    public void initMsg(String queueName) throws IOException {
        File queuePath = new File(queuePath(queueName));
        if (!queuePath.exists()) {
            boolean ok = queuePath.mkdirs();
            if (!ok) {
                //文件夹未生成
            }
        }
        File msgDataPath = new File(msgDataPath(queueName));
        if (!msgDataPath.exists()) {
            boolean mfile = msgDataPath.createNewFile();
            if (!mfile) {
                //消息数据文件未生成
            }
        }
        File msgCountPath = new File(msgCountPath(queueName));
        if (!msgCountPath.exists()) {
            boolean cfile = msgCountPath.createNewFile();
            if (!cfile) {
                //消息统计文件未生成
            }
        }
        //初始化消息统计文件
        Stat stat = new Stat();
        stat.countMsg = 0;
        stat.sumMsg = 0;
        writeMsgCount(queueName, stat);
    }
    //删除目录
    public void deleteFile(String queueName) {
        File dataFile = new File(msgDataPath(queueName));
        File countFile = new File(msgCountPath(queueName));
        File queueFile = new File(queuePath(queueName));

        boolean ok1 = dataFile.delete();
        boolean ok2 = countFile.delete();
        boolean ok3 = queueFile.delete();
        if (!ok1 || !ok2 || !ok3) {
            //有文件未删除成功，提示
        }
    }
    //判断消息文件是否存在
    public boolean isFileExists(String queueName) {
        File dataFile = new File(msgDataPath(queueName));
        if (!dataFile.exists()) {
            return false;
        }
        File countFile = new File(msgCountPath(queueName));
        if (!countFile.exists()) {
            return false;
        }
        return true;
    }
    //保存消息到文件
    //获取目录-队列上锁-确定消息长度-写入
    public void saveMsgFile(Queue queue, Message message) throws IOException {
        if (!isFileExists(queue.getName())) {
            //提示文件不存在
        }
        byte[] mByte = BinaryTool.formatByte(message);
        synchronized (queue) {
            File file = new File(msgDataPath(queue.getName()));
            message.setOffsetBeg(file.length() + 4);
            message.setOffsetEnd(file.length() + 4 + mByte.length);
            OutputStream outputStream = new FileOutputStream(msgDataPath(queue.getName()));
            DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
            dataOutputStream.writeInt(mByte.length);
            dataOutputStream.write(mByte);

            Stat stat = readMsgCount(queue.getName());
            stat.sumMsg += 1;
            stat.countMsg += 1;
            writeMsgCount(queue.getName(), stat);
        }
    }
    //删除消息，逻辑删除
    //获取目录-获取消息的位置-读取文件消息-反序列化-设置成无效-序列化-存入进去
    public void deleteMsgData(Queue queue, Message message) throws IOException, ClassNotFoundException {
        if (!isFileExists(queue.getName())) {
            //提示文件不存在
        }
        synchronized (queue) {
            // 1. 先从文件中读取对应的 Message 数据.
            RandomAccessFile randomAccessFile = new RandomAccessFile(msgDataPath(queue.getName()), "rw");
            byte[] mbyte = new byte[(int) (message.getOffsetEnd() - message.getOffsetBeg())];
            randomAccessFile.seek(message.getOffsetBeg());
            randomAccessFile.read(mbyte);
            // 2. 把当前读出来的二进制数据, 转换回成 Message 对象
            Message messageParse = (Message) BinaryTool.parseByte(mbyte);
            // 3. 把 isValid 设置为无效.
            messageParse.setIsValid((byte) 0x0);
            // 此处不需要给参数的这个 message 的 isValid 设为 0, 因为这个参数代表的是内存中管理的 Message 对象
            // 而这个对象马上也要被从内存中销毁了.
            // 4. 重新写入文件
            byte[] aByte = BinaryTool.formatByte(messageParse);

            // 虽然上面已经 seek 过了, 但是上面 seek 完了之后, 进行了读操作, 这一读, 就导致, 文件光标往后移动, 移动到
            // 下一个消息的位置了. 因此要想让接下来的写入, 能够刚好写回到之前的位置, 就需要重新调整文件光标.
            randomAccessFile.seek(message.getOffsetBeg());
            randomAccessFile.write(aByte);
        }
        //还有消息统计数据也要更新 把一个消息设为无效了, 此时有效消息个数就需要 - 1
        Stat stat = readMsgCount(queue.getName());
        if (stat.countMsg > 0) {
            stat.countMsg -= 1;
        }
        writeMsgCount(queue.getName(), stat);
    }
    // 使用这个方法, 从文件中, 读取出所有的消息内容, 加载到内存中(具体来说是放到一个链表里)
    // 这个方法, 准备在程序启动的时候, 进行调用.
    // 这里使用一个 LinkedList, 主要目的是为了后续进行头删操作.
    // 这个方法的参数, 只是一个 queueName 而不是 MSGQueue 对象. 因为这个方法不需要加锁, 只使用 queueName 就够了.
    // 由于该方法是在程序启动时调用, 此时服务器还不能处理请求呢~~ 不涉及多线程操作文件.
    public LinkedList<Message> initAllMsg(String queueName) throws IOException, ClassNotFoundException {
        LinkedList<Message> list = new LinkedList<>();
        try {
            InputStream inputStream = new FileInputStream(msgDataPath(queueName));
            DataInputStream dataInputStream = new DataInputStream(inputStream);
            //循环读取文件
            long currentIndex = 0;
            while (true) {
                int msgLength = dataInputStream.readInt();
                byte[] mbyte = new byte[msgLength];
                int readLength = dataInputStream.read(mbyte);
                if (msgLength != readLength) {
                    //读取长度和存储长度不一致
                }
                Message message = (Message) BinaryTool.parseByte(mbyte);
                //无效的信息不存储
                if (message.getIsValid() != 0x1) {
                    // 无效数据, 直接跳过.
                    // 虽然消息是无效数据, 但是 offset 不要忘记更新.
                    currentIndex += (4 + msgLength);
                    continue;
                }
                /// 5. 有效数据, 则需要把这个 Message 对象加入到链表中. 加入之前还需要填写 offsetBeg 和 offsetEnd
                //    进行计算 offset 的时候, 需要知道当前文件光标的位置的. 由于当下使用的 DataInputStream 并不方便直接获取到文件光标位置
                //    因此就需要手动计算下文件光标.
                message.setOffsetBeg(currentIndex + 4);
                message.setOffsetEnd(currentIndex + 4 + msgLength);
                currentIndex += (4 + msgLength);
                list.add(message);
            }

        } catch (EOFException e) {
            // 这个 catch 并非真是处理 "异常", 而是处理 "正常" 的业务逻辑. 文件读到末尾, 会被 readInt 抛出该异常.
            // 这个 catch 语句中也不需要做啥特殊的事情
        }
        return list;
    }

    // 检查当前是否要针对该队列的消息数据文件进行 GC
    public boolean isFlagGC(String queueName){
        Stat stat = readMsgCount(queueName);
        if (stat.sumMsg > 2000 && (double)stat.countMsg / (double) stat.sumMsg < 0.5) {
            return true;
        }
        return false;
    }

    public String msgDataPathNew(String queueName) {
        return queueName + "/queue_data_new.txt";
    }

    // 通过这个方法, 真正执行消息数据文件的垃圾回收操作.
    // 使用复制算法来完成.
    // 创建一个新的文件, 名字就是 queue_data_new.txt
    // 把之前消息数据文件中的有效消息都读出来, 写到新的文件中.
    // 删除旧的文件, 再把新的文件改名回 queue_data.txt
    // 同时要记得更新消息统计文件.
    public void gcMsgData(Queue queue) throws IOException, ClassNotFoundException {

        synchronized (queue) {
            // 由于 gc 操作可能比较耗时, 此处统计一下执行消耗的时间.
            long gcBeg = System.currentTimeMillis();
            File fileNew = new File(msgDataPath(queue.getName()));
            if (!fileNew.exists()) {
                boolean ok = fileNew.createNewFile();
                if (!ok) {
                    //提示文件未创建成功
                }
            }
            LinkedList<Message> messages = initAllMsg(queue.getName());
            OutputStream outputStream = new FileOutputStream(msgDataPathNew(queue.getName()));
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
            for (Message message : messages) {
                byte[] mbytes = BinaryTool.formatByte(message);
                objectOutputStream.writeInt(mbytes.length);
                objectOutputStream.write(mbytes);
            }

            File fileOld = new File(msgDataPath(queue.getName()));
            boolean delete = fileOld.delete();
            if (!delete) {
                //提示旧文件未删除
            }
            boolean rename = fileNew.renameTo(fileOld);
            if (!rename){
                //更改名称错误
            }
            Stat stat = readMsgCount(queue.getName());
            stat.sumMsg = messages.size();
            stat.countMsg = messages.size();
            writeMsgCount(queue.getName(), stat);

            long gcEnd = System.currentTimeMillis();
            System.out.println("[MessageFileManager] gc 执行完毕! queueName=" + queue.getName() + ", time="
                    + (gcEnd - gcBeg) + "ms");
        }
    }

}
