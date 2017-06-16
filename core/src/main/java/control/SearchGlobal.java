package control;


import com.alibaba.fastjson.JSON;
import org.apache.commons.io.IOUtils;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Vector;

class SearchGlobal {
    //single to n  implementation
    private static SearchGlobal instance;
    private SearchGlobal (){currentFolder=System.getProperty("user.dir")+ File.separator;}
    static synchronized SearchGlobal getInstance() {
        if (instance == null) {
            instance = new SearchGlobal();
        }
        return instance;
    }

    //
    private static SearchConfig cfg=null;
    private static String currentFolder;
//    private static ObjectPool<CEDD> featurePool;
    private static int processors=0;
    private static int shardSize;
    private static Vector<RTIndex> shardIndex=new Vector<>();
    private static Vector<ObjectPool<PooledSearcher>> shardSearcherPool=new Vector<>();

    private static Logger logger= LoggerFactory.getLogger(SearchGlobal.class);

    //
    private FlushThread flushThread=new FlushThread();

    boolean load(String configFile){
        processors=Runtime.getRuntime().availableProcessors();

//        try {
//            BitSampling.readHashFunctions();
//        } catch (IOException e) {
//            logger.error("Failed to initialize hash function");
//            e.printStackTrace();
//            return false;
//        }

//        String fullConfigFilePath=currentFolder+configFile;
//
//        //检查文件是否存在
//        if(!fileExsits(fullConfigFilePath)){
//            logger.error("Can not find config file: "+fullConfigFilePath);
//            return false;
//        }

        //read config
        try {
            String jsonString = IOUtils.toString(this.getClass().getResourceAsStream(configFile));
            //to config object
            cfg= JSON.parseObject(jsonString, SearchConfig.class);

        }catch(Exception e){
            e.printStackTrace();
            return false;
        }

        //trim slashes
        cfg.setIndexPath(trimSlashes(cfg.getIndexPath()));

        //shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(this::gracefullyShutdown));

        return initSearchers();
    }

    private boolean initSearchers(){

        shardSize=0;

        //detect and open shards
        while(fileExsits(cfg.getIndexPath()+"/part"+shardSize)){

            String cltPath=cfg.getIndexPath()+"/part"+shardSize;
            logger.info("Found directory: "+cltPath);
            shardIndex.add(new RTIndex(cltPath));

            GenericObjectPoolConfig gpc=new GenericObjectPoolConfig();
            gpc.setMaxIdle(processors+2);
            ObjectPool<PooledSearcher> pool=new GenericObjectPool<>(new SearcherFactory(shardSize),gpc);
            shardSearcherPool.add(pool);

            //add size
            shardSize+=1;
        }

        String shards="Local shards:";
        int docCount=0;
        for(int i=0;i<getShardSize();++i){
            shards+=" "+i+":"+getShardIndex(i).getNumDocs();
            docCount+=getShardIndex(i).getNumDocs();
        }
        shards+=" Total:"+docCount;
        logger.debug(shards);

        if(shardSize<=0){
            logger.warn("No shard index.");
            return false;
        }

        if(getNumWorkers()>0){
            logger.info("Running on BOSS mode. "+getNumWorkers()+" worker(s).");
        }else{
            logger.info("Running on WORKER mode.");
        }

        //创建feature pool
        GenericObjectPoolConfig gpc=new GenericObjectPoolConfig();
        gpc.setMaxIdle(processors*2);
//        featurePool=new GenericObjectPool<>(new CEDDFactory(),gpc);

        //开启flush thread
        flushThread.start();

        return true;
    }

    private static boolean fileExsits(String path){
        File cf=new File(path);
        return cf.exists();
    }

    int getProcessors(){return processors;}
    RTIndex getShardIndex(int i){return shardIndex.elementAt(i);}

    int getPort(){return cfg.getPort();}
    void setPort(int p){cfg.setPort(p);}
    int getMaxResults(){return cfg.getMaxResult();}
    int getShardSize(){return shardSize;}
    boolean getWriterModel(){return cfg.isWriterModel();}

    PooledSearcher borrowSearcher(int idx){
        PooledSearcher is=null;
        boolean needFlush=true;
        while(needFlush){
            try {
                is = shardSearcherPool.elementAt(idx).borrowObject();
                if(is.checkDirty()){
                    putToFlushQueue(is);
                }else{
                    needFlush=false;
                }
            }catch(Exception e){
                logger.error("Unable to borrow ImageSearcher from pool " + e.toString());
                return null;
            }
        }
        return is;
    }

    void returnSearcher(PooledSearcher pis){
        if(null!=pis) {
            if(pis.checkDirty()){
                putToFlushQueue(pis);
            }else{
                returnSearcherNoFlush(pis);
            }
        }
    }

    void returnSearcherNoFlush(PooledSearcher pis){
        if(null!=pis) {
            try {
                int idx = pis.getPoolID();
                if(idx>=0) {
                    shardSearcherPool.elementAt(idx).returnObject(pis);
                }
            }catch(Exception e){
                logger.error("Unable to return ImageSearcher to pool " + e.toString());
            }
        }
    }

    private static String trimSlashes(String in){
        String inStr=in;
        while(!inStr.isEmpty() && (inStr.endsWith("/") || inStr.endsWith("\\"))){
            inStr=inStr.substring(0,inStr.length()-1);
        }
        return inStr;
    }

    boolean addFeature(String id,String keyword){
        int index=mapIdToShard(id);
        logger.info("Add feature "+id+" to index "+index);
        boolean localSucc=shardIndex.elementAt(index).addFeature(id,keyword);


        //broadcast to workers
        if(getNumWorkers()>0){
            SearchParam sp=new SearchParam();
            sp.setId(id);
            sp.setKeyword(keyword);
            String broadStr= JSON.toJSONString(sp);
            for(String worker:cfg.getWorkers()){
                NetUtil.postString("http://"+worker+"/index",broadStr);
            }
        }

        return localSucc;
    }

    boolean deleteDocument(String id){
        int index=mapIdToShard(id);
        logger.info("Delete image "+id+" from index "+index);
        boolean localSucc=shardIndex.elementAt(index).deleteDocument(id);

        //broadcast to workers
        if(getNumWorkers()>0){
            SearchParam sp=new SearchParam();
            sp.setId(id);
            String broadStr= JSON.toJSONString(sp);
            for(String worker:cfg.getWorkers()){
                NetUtil.deleteString("http://"+worker+"/index",broadStr);
            }
        }

        return localSucc;
    }

    private void putToFlushQueue(PooledSearcher pis){
        try {
            flushThread.put(pis);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    private int mapIdToShard(String id){return Hash.hashString(id.getBytes())%shardSize;}

    private void gracefullyShutdown(){
        logger.info("Stop flush thread...");
        flushThread.interrupt();//直接终止
        logger.info("Waiting commitment...");
        for(int i=0;i<shardSize;++i){
            shardIndex.elementAt(i).shutdown();
        }
        logger.info("Gracefully shutdown.");
    }

    String getWorker(int i){return cfg.getWorkers().elementAt(i);}
    int getNumWorkers(){return cfg.getWorkers().size();}
}
