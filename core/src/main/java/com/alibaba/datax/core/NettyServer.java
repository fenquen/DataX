package com.alibaba.datax.core;

import com.alibaba.datax.common.constant.Constant;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;
import com.alibaba.datax.core.util.Global;
import com.alibaba.fastjson.JSON;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.multipart.Attribute;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class NettyServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyServer.class);

    public static void run() {
        Global.nettyThread = new Thread(() -> {
            EventLoopGroup bossGroup = new NioEventLoopGroup(1);
            EventLoopGroup workerGroup = new NioEventLoopGroup();

            try {
                ServerBootstrap serverBootstrap = new ServerBootstrap();
                serverBootstrap.group(bossGroup, workerGroup)
                        .channel(NioServerSocketChannel.class)
                        .handler(new LoggingHandler(LogLevel.INFO))
                        .childHandler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel socketChannel) throws Exception {
                                ChannelPipeline channelPipeline = socketChannel.pipeline();

                                channelPipeline.addLast(new HttpRequestDecoder());
                                channelPipeline.addLast(new HttpResponseEncoder());
                                channelPipeline.addLast(new HttpObjectAggregator(1024 * 1024));
                                channelPipeline.addLast(new CustomHttpServerHandler());
                            }
                        });

                serverBootstrap.bind(8181)
                        .sync()
                        .channel()
                        .closeFuture()
                        .sync();
            } catch (InterruptedException e) {

            } finally {
                bossGroup.shutdownGracefully();
                workerGroup.shutdownGracefully();
            }
        });

        Global.nettyThread.setDaemon(false);
        Global.nettyThread.setName("netty");

        Global.nettyThread.start();
    }

    private static class CustomHttpServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest fullHttpRequest) throws Exception {

            String uri = fullHttpRequest.uri();
            LOGGER.info(fullHttpRequest.uri());

            // 只支持post
            if (!HttpMethod.POST.equals(fullHttpRequest.method())) {
                ctx.write(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.METHOD_NOT_ALLOWED, Unpooled.EMPTY_BUFFER));
                return;
            }

            if (HttpUtil.is100ContinueExpected(fullHttpRequest)) {
                FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE, Unpooled.EMPTY_BUFFER);
                ctx.write(response);
            }

            switch (uri) {
                case Constant.NETTY_HTTP.REPORT_TG_COMM_PATH:
                    if (fullHttpRequest.content().isReadable()) {
                        String taskGroupCommunicationJson = fullHttpRequest.content().toString(CharsetUtil.UTF_8);
                        Communication taskGroupCommunication = JSON.parseObject(taskGroupCommunicationJson, Communication.class);
                        LocalTGCommunicationManager.update(taskGroupCommunication.taskGroupId, taskGroupCommunication);
                    }
                    break;
                default:
                    ctx.write(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND, Unpooled.EMPTY_BUFFER));
                    return;
            }


           /*  HttpPostRequestDecoder httpPostRequestDecoder = new HttpPostRequestDecoder(fullHttpRequest);
           httpPostRequestDecoder.offer(fullHttpRequest);
            for (InterfaceHttpData parm : httpPostRequestDecoder.getBodyHttpDatas()) {
                Attribute data = (Attribute) parm;
                // parmMap.put(data.getName(), data.getValue());
            }*/


            //  boolean keepAlive = HttpUtil.isKeepAlive(fullHttpRequest);

            FullHttpResponse httpResponse = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1,
                    HttpResponseStatus.OK,
                    Unpooled.copiedBuffer("{}", CharsetUtil.UTF_8));

            httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=UTF-8");

            // if (keepAlive) {
            httpResponse.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, httpResponse.content().readableBytes());
            //httpResponse.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            //  }

            ctx.writeAndFlush(httpResponse);

            //  if (!keepAlive) {
            //  ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
            // }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            LOGGER.error(cause.getMessage(), cause);
            ctx.close();
        }
    }
}
