package control;


import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

class NetUtil {

    private final static int MAX_DOWNLOAD_SIZE=2097152;
    private final static int DOWNLOAD_TIMEOUT_MS=5000;
    private final static int REMOTE_SEARCH_TIMEOUT_MS=1000;

    private static Logger logger= LoggerFactory.getLogger(NetUtil.class);

    private static OkHttpClient okhttp=createOkHttpClient(false);
    private static OkHttpClient okhttpDl=createOkHttpClient(true);
    private final static MediaType MEDIA_TYPE_JSON = MediaType.parse("application/json; charset=utf-8");

    private static OkHttpClient createOkHttpClient(boolean forDownload){
        int readWriteTimeout=REMOTE_SEARCH_TIMEOUT_MS;
        if(forDownload){
            readWriteTimeout=DOWNLOAD_TIMEOUT_MS;
        }
        OkHttpClient.Builder b = new OkHttpClient.Builder();
        b.connectTimeout(REMOTE_SEARCH_TIMEOUT_MS,TimeUnit.MILLISECONDS);
        b.readTimeout(readWriteTimeout, TimeUnit.MILLISECONDS);
        b.writeTimeout(readWriteTimeout, TimeUnit.MILLISECONDS);
        return b.build();
    }


    static String postString(String url,String data){
        String resp="";
        RequestBody body = RequestBody.create(MEDIA_TYPE_JSON, data);
        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .build();
        try {
            Response httpResp = okhttp.newCall(request).execute();
            resp=httpResp.body().string();
            httpResp.body().close();
        }catch(Exception e){
            logger.error("POST error:"+e.getMessage());
        }
        return resp;
    }

    static String deleteString(String url,String data){
        String resp="";
        RequestBody body = RequestBody.create(MEDIA_TYPE_JSON, data);
        Request request = new Request.Builder()
                .url(url)
                .delete(body)
                .build();
        try {
            Response httpResp = okhttp.newCall(request).execute();
            resp=httpResp.body().string();
            httpResp.body().close();
        }catch(Exception e){
            logger.error("DELETE error:"+e.getMessage());
        }
        return resp;
    }

    static String getString(String url){
        Request request = new Request.Builder()
                    .url(url)
                    .build();
        String resp="";
        try {
            Response httpResp = okhttp.newCall(request).execute();
            resp=httpResp.body().string();
            httpResp.body().close();
        }catch(Exception e){
            logger.error("GET error:"+e.getMessage());
        }
        return resp;
    }

    static byte[] downloadBytes(String src){
        if(src.isEmpty()){return null;}
        // 下载网络文件
        byte[] img=null;

        Request request = new Request.Builder()
                .url(src)
                .build();
        try{
            Response httpResp = okhttpDl.newCall(request).execute();
            if(httpResp.body().contentLength()>MAX_DOWNLOAD_SIZE){
                //too large
                logger.error("Image size is too large! limited in "+MAX_DOWNLOAD_SIZE/1024/1024+"M");
                return null;
            }
            img=httpResp.body().bytes();
            httpResp.body().close();
        }catch(Exception e){
            logger.error("DOWNLOAD error:"+e.getMessage());
        }
        return img;
    }
}
