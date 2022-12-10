package com.fenquen.datax.distribute.dispatcher;

import com.alibaba.datax.common.distribute.DispatcherInfo;
import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.*;
import java.util.Enumeration;
import java.util.concurrent.TimeUnit;

@Component
public class LifeCycle implements InitializingBean, DisposableBean {
    private static final Logger LOGGER = LoggerFactory.getLogger(LifeCycle.class);

    @Value("${multicast.address}")
    private String multicastAddress;
    @Value("${multicast.port}")
    private int multicastPort;

    @Value("${multicast.interface.address}")
    private String interfaceAddr;

    @Value("${dispatcher.message.exchange.interval.ms}")
    private int dispatcherMessageExchangeIntervalMs;

    @Value("${server.address}")
    private String serverAddress;
    @Value("${server.port}")
    private String serverPort;

    private Thread multicastSendThread;
    private Thread multicastReceiveThread;

    private MulticastSocket multicastSocket;


    @Override
    public void afterPropertiesSet() throws Exception {
        handleMulticast();
    }

    @Override
    public void destroy() throws Exception {
        multicastSendThread.interrupt();
        multicastReceiveThread.interrupt();
        multicastSocket.close();
    }

    private void handleMulticast() throws Exception {
        multicastSocket = new MulticastSocket(new InetSocketAddress("0.0.0.0", multicastPort));

        NetworkInterface targetInterface = null;
        Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
        while (networkInterfaces.hasMoreElements()) {
            NetworkInterface networkInterface = networkInterfaces.nextElement();

            Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
            while (inetAddresses.hasMoreElements()) {
                InetAddress inetAddress = inetAddresses.nextElement();
                if (inetAddress.getHostAddress().equals(interfaceAddr)) {
                    targetInterface = networkInterface;
                }
            }
        }
        if (null == targetInterface) {
            throw new DispatcherException(String.format("没有找到%s对应的interface", interfaceAddr));
        }

        InetSocketAddress multicast = new InetSocketAddress(multicastAddress, multicastPort);

        multicastSocket.joinGroup(multicast, targetInterface);

        multicastSendThread = new Thread(() -> {
            try {
                while (true) {
                    try {
                        DispatcherInfo dispatcherInfo = new DispatcherInfo(serverAddress, serverPort);
                        Global.HOST_PORT_DISPATCHER_INFO.put(dispatcherInfo.host + ":" + dispatcherInfo.port, dispatcherInfo);
                        byte[] byteArr = JSON.toJSONString(dispatcherInfo).getBytes("utf-8");
                        DatagramPacket datagramPacketSend = new DatagramPacket(byteArr, byteArr.length, multicast);

                        multicastSocket.send(datagramPacketSend);
                        TimeUnit.MILLISECONDS.sleep(dispatcherMessageExchangeIntervalMs);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        });
        multicastSendThread.start();

        multicastReceiveThread = new Thread(() -> {
            DatagramPacket datagramPacketReceive = new DatagramPacket(new byte[1024], 1024);

            try {
                while (true) {
                    multicastSocket.receive(datagramPacketReceive);

                    String json = new String(datagramPacketReceive.getData(), 0, datagramPacketReceive.getLength());
                    //LOGGER.info(json);

                    DispatcherInfo dispatcherInfo = JSON.parseObject(json, DispatcherInfo.class);
                    Global.HOST_PORT_DISPATCHER_INFO.put(dispatcherInfo.host + ":" + dispatcherInfo.port, dispatcherInfo);
                }
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        });
        multicastReceiveThread.start();

    }
}
