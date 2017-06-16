package control;

import com.alibaba.fastjson.JSON;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.apache.commons.codec.binary.Base64;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;


class SearchHandler extends SimpleChannelInboundHandler<HttpObject> {

    class ClusterResult implements Comparable<ClusterResult>{
        String id;
        double score;
        String keyword;
        ClusterResult(String id,String keyword){
            this.id=id;
//            this.score=score;
            this.keyword=keyword;
        }

        @Override
        public int compareTo(ClusterResult o) {
            if(score>o.score){
                return -1;
            }else if(score<o.score){
                return 1;
            }else{
                return id.compareTo(o.id);
            }
        }

        /*
        @Override
        public boolean equals(Object obj) {
            // it's not the same if it's not the same class.
            return obj instanceof ClusterResult && id.equals(((ClusterResult)obj).id);
        }
        */
    }

    class ClusterImageSearchHits{
        ArrayList<ClusterResult> results;
        ClusterImageSearchHits(Collection<ClusterResult> results){
            this.results = new ArrayList<>(results.size());
            this.results.addAll(results);
        }
        double score(int position) {return results.get(position).score;}
        String document(int position) {return results.get(position).id;}
        String keyword(int position){return results.get(position).keyword;}
        int length(){return results.size();}
    }

    private final StringBuilder respContent = new StringBuilder();
    private final static String RESPONSE_CONTENT_TYPE="application/json; charset=UTF-8";
    private final static String ALIVE_RESPONSE="{\"alive\":true}";
    private final static int ALL_SHARDS=-1;
    private final static int SEARCH_TIMEOUT_MS=3000;

    private final static SearchGlobal SEARCH_GLOBAL=SearchGlobal.getInstance();
    private Logger logger= LoggerFactory.getLogger(SearchHandler.class);
    private ExecutorService threadPool = Executors.newCachedThreadPool();
    private SortedSet<ClusterResult> docs = Collections.synchronizedSortedSet(new TreeSet<>());

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        ctx.close();
        cause.printStackTrace();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
        messageReceived(ctx, msg);
    }

    private void messageReceived(ChannelHandlerContext ctx,
                                HttpObject msg) throws Exception {

        FullHttpRequest request=(FullHttpRequest) msg;

        if (!request.decoderResult().isSuccess()) {
            sendError(ctx, BAD_REQUEST);
            return;
        }

        respContent.setLength(0);//clear response

        if(request.uri().compareToIgnoreCase("/search")==0
                && request.method()== HttpMethod.POST){
            doHandleSearch(ctx,request);
        }
        if(request.uri().compareToIgnoreCase("/index")==0
                && request.method()== HttpMethod.POST){
            doHandleAddIndex(ctx,request);
        }
        if(request.uri().compareToIgnoreCase("/index")==0
                && request.method()== HttpMethod.DELETE){
            doHandleDeleteIndex(ctx,request);
        }
        if(request.uri().compareToIgnoreCase("/")==0
                && request.method()== HttpMethod.GET ){
            doHandleHome();
        }
        if(request.uri().compareToIgnoreCase("/alive")==0
                && request.method()== HttpMethod.GET ){
            doHandleAlive();
        }
        if(request.uri().compareToIgnoreCase("/workers")==0
                && request.method()== HttpMethod.GET ){
            doHandleWorkers();
        }

        //write and flush
        resp(ctx);
    }

    private void resp(ChannelHandlerContext ctx){
        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1,
                HttpResponseStatus.OK, Unpooled.copiedBuffer(respContent.toString(), CharsetUtil.UTF_8));
        response.headers().set(CONTENT_TYPE, RESPONSE_CONTENT_TYPE);
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);

    }

    private static void sendError(ChannelHandlerContext ctx,
                                  HttpResponseStatus status) {
        String ret =  "{}";
        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1,
                status, Unpooled.copiedBuffer(ret, CharsetUtil.UTF_8));
        response.headers().set(CONTENT_TYPE, RESPONSE_CONTENT_TYPE);
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    private void doHandleHome() throws Exception {
        respContent.append("{\"whatisthis\":\"hc lucene search server\"}");
    }

    private void doHandleAlive() throws Exception {
        respContent.append(ALIVE_RESPONSE);
    }

    private void doHandleWorkers() throws Exception {
        int numWorkers=SEARCH_GLOBAL.getNumWorkers();
        if(numWorkers<=0) {
            respContent.append("{\"alive\":0,\"total\":0}");
        }else{
            int aliveCount=0;
            for(int i=0;i<numWorkers;++i){
                if(remoteAlive(i)){
                    aliveCount+=1;
                }
            }
            respContent.append("{\"alive\":").append(aliveCount).append(",\"total\":").append(numWorkers).append("}");
        }
    }

    private void doHandleAddIndex(ChannelHandlerContext ctx,
                                FullHttpRequest request) throws Exception{
        ByteBuf buf =  request.content();
        byte[] req = new byte[buf.readableBytes()];
        buf.readBytes(req);
        //decode json
        SearchParam sp=decodeSearchParam(req);
        if(sp==null) {
            sendError(ctx, BAD_REQUEST);
            return;
        }

        long startTime = System.currentTimeMillis();
        String id=sp.getId();
        String keyword=sp.getKeyword();
        //tags=tags.replaceAll(",|:|/"," ");//spliter to spaces
        //tags=tags.replaceAll("  "," ");//merge double spaces
        boolean succ=false;
        if(!keyword.isEmpty()){
            succ=SEARCH_GLOBAL.addFeature(id,keyword);
        }

        //build output
        long consumingTime = System.currentTimeMillis() - startTime;
        respContent.append("{\"took\":").append(consumingTime).append(",");
        if(succ) {
            respContent.append("\"success\":").append("true").append("}");
        }else{
            respContent.append("\"success\":").append("false").append("}");
        }
    }

    private void doHandleDeleteIndex(ChannelHandlerContext ctx,
                                     FullHttpRequest request) throws Exception{
        ByteBuf buf =  request.content();
        byte[] req = new byte[buf.readableBytes()];
        buf.readBytes(req);
        //decode json
        SearchParam sp=decodeSearchParam(req);
        if(sp==null) {
            sendError(ctx, BAD_REQUEST);
            return;
        }
        long startTime = System.currentTimeMillis();
        String id=sp.getId();
        boolean succ=false;
        if(!id.isEmpty()){
            succ=SEARCH_GLOBAL.deleteDocument(id);//do delete
        }
        long consumingTime = System.currentTimeMillis() - startTime;
        respContent.append("{\"took\":").append(consumingTime).append(",");
        if(succ) {
            respContent.append("\"success\":").append("true").append("}");
        }else{
            respContent.append("\"success\":").append("false").append("}");
        }

    }

    private void doHandleSearch(ChannelHandlerContext ctx,
                              FullHttpRequest request) throws Exception{
        ByteBuf buf =  request.content();
        byte[] req = new byte[buf.readableBytes()];
        buf.readBytes(req);
        //decode json
        SearchParam sp=decodeSearchParam(req);
        if(sp==null) {
            sendError(ctx, BAD_REQUEST);
            return;
        }

        long startTime = System.currentTimeMillis();

        ClusterImageSearchHits hits=null;

        String id=sp.getId();
        String keyword=sp.getKeyword();
        int shard=sp.getShard();
        //如果多个参数同时存在
        //搜索图像采用的优先级是: id>imagef>image>url (服务器接收的数据量从小到大)
//        if(!id.isEmpty()){
//            hits=searchById(id,tags);
//        } else
        if(!keyword.isEmpty()){
            if(shard>=0){
                hits=searchByKeyword(keyword,shard);//检索特定分片
            }else{
                hits=searchByKeyword(keyword,ALL_SHARDS);//检索所有分片
            }
        }

        if(hits==null){
            sendError(ctx, INTERNAL_SERVER_ERROR);
            return;
        }

        long consumingTime = System.currentTimeMillis() - startTime;

        //build output
        int resultsCount=hits.length()<SEARCH_GLOBAL.getMaxResults()?hits.length():SEARCH_GLOBAL.getMaxResults();

        respContent.append("{\"took\":").append(consumingTime).append(",");
        respContent.append("\"hits\":").append(resultsCount).append(",");
        respContent.append("\"results\":[");

        for(int i=0;i<resultsCount;i++){
//            java.text.DecimalFormat df=new java.text.DecimalFormat("#0.00000000");
//            String score=df.format(hits.score(i));
//            respContent.append("{\"score\":").append(score).append(",");

            String idName = hits.document(i);
            respContent.append("\"id\":\"").append(idName).append("\",");

            String hitkeyword = hits.keyword(i);
            respContent.append("\"keyword\":\"").append(hitkeyword).append("\"}");

            if(i<resultsCount-1){
                respContent.append(",");
            }
        }
        respContent.append("]");
        respContent.append("}");
    }

    private ClusterImageSearchHits searchByKeyword(String keyword, int shard) {
        docs.clear();

        if(shard<0){
            //参数是检索所有分片
            int numWorkers=SEARCH_GLOBAL.getNumWorkers();
            if(numWorkers<=0){
                //workers <= 0
                //find in local index
                for(int i=0;i<SEARCH_GLOBAL.getShardSize();++i){
                    int poolID=i;
                    threadPool.execute(() -> searchLocal(keyword,poolID));
                }
            }else{
                //workers > 0
                //find in remote
                int numShards=SEARCH_GLOBAL.getShardSize();
                for(int i=0;i<numShards;++i){
                    int poolID=i;
                    int remoteHash=(Hash.hashStringTypeA(keyword.getBytes())+i)%numWorkers;
                    String address=SEARCH_GLOBAL.getWorker(remoteHash);
                    threadPool.execute(() -> searchRemote(keyword,poolID,address));
                }
            }

            //wait for threads
            try{
                threadPool.shutdown();
                if(!threadPool.awaitTermination(SEARCH_TIMEOUT_MS, TimeUnit.MILLISECONDS)){
                    threadPool.shutdownNow();
                }
            }catch(Exception e){
                logger.error("Search thread error.");
            }
        }
//        else{
//            //参数是检索特定分片,只在本地检索
//            PooledImageSearcher searcher=SEARCH_GLOBAL.borrowImageSearcher(shard);
//            RTIndex rtReader=SEARCH_GLOBAL.getShardIndex(shard);
//            ImageSearchHits partHits=null;
//            try {
//                partHits=searcher.getSearcher().search(feature,tags);
//            } catch (IOException e) {
//                SEARCH_GLOBAL.returnImageSearcher(searcher);
//                e.printStackTrace();
//            }
//            SEARCH_GLOBAL.returnImageSearcher(searcher);
//            if(partHits!=null){
//                for(int hit=0;hit<partHits.length();++hit){
//                    String[] ids=rtReader.getID(partHits.documentID(hit));
//
//                    for(String iid:ids){
//                        docs.add(new ClusterResult(
//                                iid,
//                                partHits.score(hit),
//                                rtReader.getTags(partHits.documentID(hit)),
//                                rtReader.getKeyword(partHits.documentID(hit))
//                        ));
//                    }
//
//                }
//            }
//        }

//        SEARCH_GLOBAL.returnFeature(feature);

        return new ClusterImageSearchHits(docs);
    }

    private void searchLocal(String keyword,int poolID){
        PooledSearcher searcher=SEARCH_GLOBAL.borrowSearcher(poolID);
        RTIndex rtReader=SEARCH_GLOBAL.getShardIndex(poolID);
        ScoreDoc[] partHits=null;
        try {
            partHits=searcher.getSearcher().search(keyword);
        } catch (IOException e) {
            SEARCH_GLOBAL.returnSearcher(searcher);
            e.printStackTrace();
        } catch (ParseException e) {
            SEARCH_GLOBAL.returnSearcher(searcher);
            e.printStackTrace();
        }
        SEARCH_GLOBAL.returnSearcher(searcher);
        if(partHits!=null){
            for(int hit=0;hit<partHits.length;++hit){
                String[] ids=rtReader.getID(partHits[hit].doc);
                for(String iid:ids){
                    docs.add(new ClusterResult(iid,
                            rtReader.getKeyword(partHits[hit].doc
                            )));
                }
            }
        }
    }

    private void searchRemote(String keyword, int poolID, String worker){
        SearchParam sp=new SearchParam();
        sp.setShard(poolID);
        sp.setKeyword(keyword);
        String jsonStr= JSON.toJSONString(sp);
        //logger.debug("Remote query: "+jsonStr);
        //post request
        Long begin = System.currentTimeMillis();
        String response=NetUtil.postString("http://"+worker+"/search",jsonStr);
        Long end = System.currentTimeMillis();
        logger.info(worker+" - "+(end-begin));
        if(response.isEmpty()){
            return;
        }

        JsonResultObject jro=null;
        try {
            jro = JSON.parseObject(response,JsonResultObject.class);
        }catch(Exception e){
            logger.warn("Remote server return error or empty: "+response);
        }

        if(null==jro || jro.getHits()==0){
            return;
        }
        //get results and add to docs
        for(int hit=0;hit<jro.getHits();++hit){
            HitEntry he=jro.getResults().get(hit);
            docs.add(new ClusterResult(he.getId(),he.getKeyword()));
        }
    }

    private boolean remoteAlive(int poolID){
        if(poolID<0 || poolID>=SEARCH_GLOBAL.getShardSize()){
            return false;
        }

        String worker=SEARCH_GLOBAL.getWorker(poolID);
        String response=NetUtil.getString("http://"+worker+"/alive");
        return !response.isEmpty() && response.equals(ALIVE_RESPONSE);
    }



    private SearchParam decodeSearchParam(byte[] buf){

        //decode json
        SearchParam sp=null;
        try {
            String jsonStr = new String(buf, CharsetUtil.UTF_8);
            //to config object
            sp= JSON.parseObject(jsonStr, SearchParam.class);
        }catch(Exception e) {
            e.printStackTrace();
        }

        return sp;
    }
}
