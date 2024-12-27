package com.liyc.mqs.mqserver;

import com.liyc.mqs.common.*;
import com.liyc.mqs.mqserver.core.BasicProperties;
import com.liyc.mqs.mqserver.tool.BinaryTool;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 这个 BrokerServer 就是咱们 消息队列 本体服务器.
 * 本质上就是一个 TCP 的服务器.
 * 属性
 * 1.ServerSocker-当前服务
 * 2.sessions-存储客户端连接服务器的socket
 * 3.virtualhost的对象，用来实现核心API
 * 4.设置一个进程启停标志
 * 5.线程池处理客户端连接
 * 方法
 * 1.启动服务
 * 2.停止服务
 * 3.线程池处理连接
 * 4.处理连接发来的请求
 * 5.读请求，readRequest
 * 6.写响应，writeResponse
 * 7.清除连接
 *
 * @author Liyc
 * @date 2024/12/26 11:24
 **/

public class BrokerServer {
    //ServerSocker-当前服务
    private ServerSocket serverSocket = null;
    //sessions-存储客户端连接服务器的socket
    private ConcurrentHashMap<String, Socket> sessions = new ConcurrentHashMap<>();
    //virtualhost的对象，用来实现核心API
    private VirtualHost virtualHost = new VirtualHost("default");
    //设置一个进程启停标志
    private volatile boolean runable = true;
    //线程池处理客户端连接
    private ExecutorService executorService = null;

    public BrokerServer(int port) throws IOException {
        serverSocket = new ServerSocket(port);
    }
    /**
     * 启动服务
     */
    public void start() throws IOException {
        System.out.println("[BrokerServer] BrokerServer启动监听");
        executorService = Executors.newCachedThreadPool();
        while (runable) {
            //监听端口
            Socket socket = serverSocket.accept();
            //通过线程池处理连接
            executorService.submit(() -> {
                connectionProcess(socket);
            });
        }
    }

    /**
     * 停止服务
     */
    public void stop() throws IOException {
        System.out.println("[BrokerServer] BrokerServer停止服务");
        runable = false;
        sessions.clear();
        serverSocket.close();

    }

    /**
     * 线程池处理连接
     * @param clientSocket
     */
    public void connectionProcess(Socket clientSocket) {
        try {
            InputStream inputStream = clientSocket.getInputStream();
            OutputStream outputStream = clientSocket.getOutputStream();
            DataInputStream dataInputStream = new DataInputStream(inputStream);
            DataOutputStream dataOutputStream = new DataOutputStream(outputStream);

            //1.解析请求报文
            Request request = readRequest(dataInputStream);
            //2.请求处理
            Response response = process(request, clientSocket);
            //3.返回响应
            writeResponse(dataOutputStream, response);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (MqException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 处理连接发来的请求
     * @param request
     * @param clientSocket
     */
    public Response process(Request request, Socket clientSocket) throws MqException, IOException, ClassNotFoundException {

        BasicArguments basicArguments = (BasicArguments) BinaryTool.parseByte(request.getPayload());
        boolean ok = true;
        //1.不同消息类型的处理
        if (request.getType() == 0x1) {
            //创建连接请求
            sessions.put(basicArguments.getChannelId(), clientSocket);
        } else if (request.getType() == 0x2) {
            //销毁连接请求
            clearSessions(clientSocket);
        } else if (request.getType() == 0x3) {
            //交换机创建请求
            ExchangeDeclareArguments exchange = (ExchangeDeclareArguments) basicArguments;
            ok = virtualHost.exchangeDeclare(exchange.getExchangeName(), exchange.getType(), exchange.isDurable(), exchange.isAutoDelete(), exchange.getArguments());
        } else if (request.getType() == 0x4) {
            //交换机删除请求
            ExchangeDeleteArguments exchange = (ExchangeDeleteArguments) basicArguments;
            ok = virtualHost.exchangeDelete(exchange.getExchangeName());
        } else if (request.getType() == 0x5) {
            //队列创建请求
            QueueDeclareArguments queue = (QueueDeclareArguments) basicArguments;
            ok = virtualHost.queueDeclare(queue.getQueueName(), queue.isExclusive(), queue.isDurable(), queue.isAutoDelete(), queue.getArguments());
        } else if (request.getType() == 0x6) {
            //队列删除请求
            QueueDeleteArguments queue = (QueueDeleteArguments) basicArguments;
            ok = virtualHost.queueDelete(queue.getQueueName());
        } else if (request.getType() == 0x7) {
            //绑定创建请求
            QueueBindArguments binding = (QueueBindArguments) basicArguments;
            ok = virtualHost.bindingDeclare(binding.getExchangeName(), binding.getQueueName(), binding.getBindingKey());
        } else if (request.getType() == 0x8) {
            //绑定删除请求
            QueueUnbindArguments binding = (QueueUnbindArguments) basicArguments;
            ok = virtualHost.bindingDelete(binding.getExchangeName(), binding.getQueueName());
        } else if (request.getType() == 0x9) {
            //消息发布
            BasicPublishArguments basicPublish = (BasicPublishArguments) basicArguments;
            ok = virtualHost.basicPublic(basicPublish.getExchangeName(), basicPublish.getRoutingKey(), basicPublish.getBasicProperties(), basicPublish.getBytes());
        } else if (request.getType() == 0xa) {
            //消息消费
            BasicConsumeArguments basicConsume = (BasicConsumeArguments) basicArguments;
            ok = virtualHost.basicConsume(basicConsume.getConsumerTag(), basicConsume.getQueueName(), basicConsume.isAutoAck(), new Consumer() {
                @Override
                public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] bytes) throws IOException {
                    //添加消费者的消费方法
                    //1.根据consumerTag获取客户端
                    Socket socket = sessions.get(consumerTag);
                    if (socket == null || socket.isClosed()) {
                        System.out.println("[BrokerServer] ClientSocket停止服务，无法消费消息！");
                    }

                    //2.格式要发送的消息
                    SubScribeReturns subReturns = new SubScribeReturns();
                    subReturns.setChannelId(consumerTag);
                    subReturns.setRid("");// 由于这里只有响应, 没有请求, 不需要去对应. rid 暂时不需要.
                    subReturns.setOk(true);
                    subReturns.setConsumerTag(consumerTag);
                    subReturns.setBasicProperties(basicProperties);
                    subReturns.setBody(bytes);

                    byte[] mbytes = BinaryTool.formatByte(subReturns);
                    Response responseClient = new Response();
                    responseClient.setType(0xc);
                    responseClient.setLength(mbytes.length);
                    responseClient.setPayload(mbytes);

                    //3.发送消息
                    OutputStream outputStream = socket.getOutputStream();
                    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
                    writeResponse(dataOutputStream, responseClient);
                }
            });
        } else if (request.getType() == 0xb) {
            //消息确认
            BasicAckArguments basicAck = (BasicAckArguments) basicArguments;
            ok = virtualHost.basicAck(basicAck.getQueueName(), basicAck.getMessageId());
        } else {
            // 当前的 type 是非法的.
            throw new MqException("[BrokerServer] 未知的 type! type=" + request.getType());
        }
        //设置返回信息
        BasicReturns result = new BasicReturns();
        result.setChannelId(basicArguments.getChannelId());
        result.setRid(basicArguments.getRid());
        result.setOk(ok);

        byte[] rbyte = BinaryTool.formatByte(request);
        Response response = new Response();
        response.setType(0xd);
        response.setLength(rbyte.length);
        response.setPayload(rbyte);
        System.out.println("[Response] rid=" + result.getRid() + ", channelId=" + result.getChannelId()
                + ", type=" + response.getType() + ", length=" + response.getLength());
        return response;
    }

    /**
     * 读请求，readRequest
     * @param dataInputStream
     * @return
     */
    public Request readRequest(DataInputStream dataInputStream) throws IOException {
        Request request = new Request();
        request.setType(dataInputStream.readInt());
        request.setLength(dataInputStream.readInt());
        byte[] bytes = new byte[request.getLength()];
        int n = dataInputStream.read(bytes);
        if (n != request.getLength()) {
            System.out.println("[BrokerServer] request请求报文错误");
            throw new IOException("request请求报文读取错误");
        }
        request.setPayload(bytes);
        return request;
    }

    /**
     * 写响应，writeResponse
     * @param dataOutputStream
     * @param response
     */
    public void writeResponse(DataOutputStream dataOutputStream, Response response) throws IOException {
        dataOutputStream.writeInt(response.getType());
        dataOutputStream.writeInt(response.getLength());
        dataOutputStream.write(response.getPayload());
        //这个刷新缓冲区也是重要的操作!!
        dataOutputStream.flush();
    }

    /**
     * 清除连接
     * @param socket
     */
    public void clearSessions(Socket socket) {
        List<String> so = new ArrayList<>();
        //不能一边遍历一边删除，会有问题，分开操作
        for (Map.Entry<String, Socket> map : sessions.entrySet()) {
            if (socket == map.getValue()) {
                // 不能在这里直接删除!!!
                // 这属于使用集合类的一个大忌!!! 一边遍历, 一边删除!!!
                // sessions.remove(entry.getKey());
                so.add(map.getKey());
            }
        }
        for (String s : so) {
            sessions.remove(s);
        }
        System.out.println("[BrokerServer] 清理 session 完成! 被清理的 channelId=" + so);
    }
}
