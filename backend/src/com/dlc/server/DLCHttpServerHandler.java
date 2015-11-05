/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dlc.server;

import com.dlc.backend.Controller;
import com.google.gson.Gson;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;

import static io.netty.handler.codec.http.HttpHeaderNames.*;
import io.netty.handler.codec.http.HttpMethod;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.*;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Sharable
public class DLCHttpServerHandler extends ChannelHandlerAdapter {

    private final StringBuilder buf = new StringBuilder();
    private Controller c = new Controller();
    private final Gson gson = new Gson();
    
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof HttpRequest) {
            handleHttpRequest(ctx, (HttpRequest) msg);
        }
    }

    private void handleHttpRequest(ChannelHandlerContext ctx, HttpRequest req) {
        QueryStringDecoder uri = new QueryStringDecoder(req.uri());
        Map<String, List<String>> params = uri.parameters();
        if (uri.path().equals("/search")) {
            doSearch(ctx, req, params);
        }
    }

    private void doSearch(ChannelHandlerContext ctx, HttpRequest req, Map<String, List<String>> params) {
        if (req.method() == HttpMethod.GET) {
            String keyword = params.get("key").get(0);
            System.out.println("searching for: " + keyword);
            ArrayList<String> matched_files;
            matched_files = c.search(keyword);
            
            writeResponse(ctx, gson.toJson(matched_files));
        }
    }

    private void writeResponse(ChannelHandlerContext ctx, String json) {
        buf.setLength(0);
        buf.append(json);
        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK,
                Unpooled.copiedBuffer(buf, CharsetUtil.UTF_8));
        response.headers().set(CONTENT_TYPE, "text/plain");
        response.headers().setInt(CONTENT_LENGTH, response.content().readableBytes());
        response.headers().set(ACCESS_CONTROL_ALLOW_ORIGIN, "*");

        ctx.write(response).addListener(ChannelFutureListener.CLOSE);

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }

}
