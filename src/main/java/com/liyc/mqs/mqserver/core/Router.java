package com.liyc.mqs.mqserver.core;

import java.util.Objects;

/**
 * 路由转发规则类
 *
 * @author Liyc
 * @date 2024/12/23 15:19
 **/

public class Router {

    //BindingKey规则校验
    public boolean checkBindingKey(String bindingKey) {
        String[] words = bindingKey.split("\\.");
        //1、判断是否为空和长度
        if (words == null || words.length == 0) {
            // 空字符串, 也是合法情况. 比如在使用 direct / fanout 交换机的时候, bindingKey 是用不上的.
            return true;
        }

        for (int j=0; j < words.length; j++) {
            //2、判断是否有特殊字符
            for (int i=0; i < words[j].length(); i++) {
                if (words[j].charAt(i) >= 'A' && words[j].charAt(i) <= 'Z') {
                    continue;
                }
                if (words[j].charAt(i) >= 'a' && words[j].charAt(i) <= 'z') {
                    continue;
                }
                if (words[j].charAt(i) >= '0' && words[j].charAt(i) <= '9') {
                    continue;
                }
                if (words[j].charAt(i) == '_') {
                    continue;
                }
                /**
                 * 3、不允许出现字母+*或则字母+#号的组合
                 * aaa.*bb.ccc
                 */
                if ((words[j].charAt(i) == '*' && words[j].equals("*")) || (words[j].charAt(i) == '#' && words[j].equals("#")) ) {
                    continue;
                }
                return false;
            }


            /**4、多个匹配符时，默认按此规则来
             * 为啥这么约定? 因为前三种相邻的时候, 实现匹配的逻辑会非常繁琐, 同时功能性提升不大~~
             * #,#  false
             * #,*  false
             * *,#  false
             * *,*  true
             */
            if (j < words.length - 1) {
                if (words[j].equals("#") && words[j+1].equals("#")){
                    return false;
                }
                if (words[j].equals("#") && words[j+1].equals("*")){
                    return false;
                }
                if (words[j].equals("*") && words[j+1].equals("#")){
                    return false;
                }
            }
        }
        return true;
    }

    //RoutingKey规则校验
    public boolean checkRoutingKey(String routingKey) {
        String[] words = routingKey.split("\\.");
        //1、判断是否为空和长度
        if (words == null || words.length == 0) {
            // 空字符串, 也是合法情况. 比如在使用 direct / fanout 交换机的时候, bindingKey 是用不上的.
            return true;
        }

        for (int j=0; j < words.length; j++) {
            //2、判断是否有特殊字符
            for (int i = 0; i < words[j].length(); i++) {
                if (words[j].charAt(i) >= 'A' && words[j].charAt(i) <= 'Z') {
                    continue;
                }
                if (words[j].charAt(i) >= 'a' && words[j].charAt(i) <= 'z') {
                    continue;
                }
                if (words[j].charAt(i) >= '0' && words[j].charAt(i) <= '9') {
                    continue;
                }
                if (words[j].charAt(i) == '_') {
                    continue;
                }
                /**
                 * 3、不允许出现字母+*或则字母+#号的组合
                 * aaa.*bb.ccc
                 */
                if ((words[j].charAt(i) == '*' && words[j].equals("*")) || (words[j].charAt(i) == '#' && words[j].equals("#"))) {
                    continue;
                }
                return false;
            }
        }
        return true;
    }

    /**转发规则
     * DIRECT-直接交换机：routingKey指定哪个队列，就走哪个队列
     * FANOUT-扇出交换机：匹配当前交换机下所有队列
     * TOPIC-主题交换机：routingKey匹配bingdingKey的相关队列
     * @param exchangeType
     * @param binding
     * @param message
     * @return
     */
    public boolean route(ExchangeType exchangeType, Binding binding, Message message) {
        if (ExchangeType.FANOUT.equals(exchangeType)) {
            return true;
        } else if (ExchangeType.TOPIC.equals(exchangeType)) {
            return routeTopic(binding, message);
        }
        return false;
    }

    /**
     * 详细转发
     * 第一种情况：routingKey和bindingKey一模一样
     * 第二种情况：有*匹配
     * 第三种情况：有#匹配
     *
     * @param binding
     * @param message
     * @return
     */
    public boolean routeTopic(Binding binding, Message message) {
        String[] bindKeys = binding.getBindingKey().split("\\.");
        String[] routeKeys = message.getRoutingKey().split("\\.");
        //第一种情况：routingKey和bindingKey一模一样
        if (Objects.equals(binding.getBindingKey(), message.getRoutingKey())) {
            return true;
        }
        int bindIndex = 0;
        int routeIndex = 0;
        while (bindIndex < bindKeys.length && routeIndex < routeKeys.length) {

            if (bindKeys[bindIndex].equals(routeKeys[routeIndex])) {
                bindIndex++;
                routeIndex++;
                continue;
            } else if (bindKeys[bindIndex].equals("*")) {
                //第二种情况：有*匹配，*只匹配一个，后续继续往下匹配
                bindIndex++;
                routeIndex++;
                continue;
            } else if (bindKeys[bindIndex].equals("#")) {
                //第三种情况：有#匹配。如果#号后面没有了，那就直接匹配；如果后面还有就匹配routingKey和bindingKey相等的地方继续往下匹配
                bindIndex++;
                if (bindIndex >= bindKeys.length) {
                    return true;
                }
                for (int i=routeIndex; i < routeKeys.length; i++) {
                    if (bindKeys[bindIndex].equals(routeKeys[i])){
                        routeIndex = i;
                        break;
                    }
                }
            } else {
                return false;
            }
        }
        //最后循环完，如果都到末尾了，那就是相等的
        if (routeIndex == bindKeys.length && routeIndex == routeKeys.length) {
            return true;
        }
        return false;
    }
}
