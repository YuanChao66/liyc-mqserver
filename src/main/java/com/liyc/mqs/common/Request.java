package com.liyc.mqs.common;

/**
 * 表示一个网络通信中的请求对象. 按照自定义协议的格式来展开的
 * 属性：1、类型 2、长度 3、正文
 *
 * @author Liyc
 * @date 2024/12/25 17:51
 **/

public class Request {
    private int type;
    private int length;
    private byte[] payload;

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }
}
