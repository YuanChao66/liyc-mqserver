package com.liyc.mqs.mqserver.tool;

import java.io.*;

/**
 * 序列化工具类
 * 序列化方法-formatByte
 * 反序列化方法-parseByte
 *
 * @author Liyc
 * @date 2024/12/13 16:21
 **/

public class BinaryTool {
    //序列化方法-把一个对象序列化成一个字节数组
    public static byte[] formatByte(Object object) throws IOException {
        // 这个流对象相当于一个变长的字节数组.
        // 就可以把 object 序列化的数据给逐渐的写入到 byteArrayOutputStream 中, 再统一转成 byte[]
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        // 此处的 writeObject 就会把该对象进行序列化, 生成的二进制字节数据, 就会写入到ObjectOutputStream 中.
        // 由于 ObjectOutputStream 又是关联到了 ByteArrayOutputStream, 最终结果就写入到 ByteArrayOutputStream 里了
        objectOutputStream.writeObject(object);
        // 这个操作就是把 byteArrayOutputStream 中持有的二进制数据取出来, 转成 byte[]
        return byteArrayOutputStream.toByteArray();
    }

    //反序列化方法-把一个字节数组, 反序列化成一个对象
    public static Object parseByte(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        try (ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
            // 此处的 readObject, 就是从 data 这个 byte[] 中读取数据并进行反序列化.
            return objectInputStream.readObject();
        }
    }
}
