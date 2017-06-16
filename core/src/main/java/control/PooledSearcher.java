package control;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PooledSearcher{
    private  RTSearcher innerSearcher;
    private int poolID=-1;
    private final static Logger logger= LoggerFactory.getLogger(PooledSearcher.class);

    PooledSearcher(int maxHits,RTIndex reader,int pool) {
        innerSearcher=new RTSearcher(maxHits,reader);
        poolID=pool;
    }

    RTSearcher getSearcher(){
        return innerSearcher;
    }

    int getPoolID(){return poolID;}

    boolean checkDirty(){
        return innerSearcher.checkDirty();
    }

    private void doFlush(){
        logger.info("Reopen reader for image searcher " + this.toString());
        innerSearcher = new RTSearcher(
                SearchGlobal.getInstance().getMaxResults(),
                SearchGlobal.getInstance().getShardIndex(poolID));
    }

    void reopenIfNeeded(){
        if(checkDirty()){
           doFlush();
        }
    }
}
