package control;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;

class SearchInitializer extends ChannelInitializer<SocketChannel> {
    @Override
    public void initChannel(SocketChannel ch) throws Exception {
        // Create a default pipeline implementation.
        ChannelPipeline pipeline = ch.pipeline();


        //http服务器端对request解码
        pipeline.addLast("http-decoder", new HttpRequestDecoder());

        // 聚合器，把多个消息转换为一个单一的FullHttpRequest或是FullHttpResponse
        pipeline.addLast("http-aggregator", new HttpObjectAggregator(Define.MAX_CONTENT_LENGTH));

        //http服务器端对response编码
        pipeline.addLast("http-encoder", new HttpResponseEncoder());

        // 压缩
        pipeline.addLast("deflater", new HttpContentCompressor());

        //请求处理
        pipeline.addLast("handler", new SearchHandler());
    }
}