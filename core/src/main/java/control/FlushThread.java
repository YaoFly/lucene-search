package control;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

class FlushThread extends Thread{
    private final static SearchGlobal SEARCH_GLOBAL=SearchGlobal.getInstance();
    private final static Logger logger= LoggerFactory.getLogger(FlushThread.class);
    private BlockingQueue<PooledSearcher> flushQueue = new LinkedBlockingQueue<>(128);
    private boolean quit=false;
    public void run() {
        while(!quit){
            try {
                PooledSearcher pis = consume();
                pis.reopenIfNeeded();
                SEARCH_GLOBAL.returnSearcherNoFlush(pis);
            }catch(Exception e){
                if(!( e instanceof InterruptedException)){
                    e.printStackTrace();
                }
            }
        }
    }

    // 生产对象，放入队列
    boolean put(PooledSearcher pis) throws InterruptedException {
        logger.debug("Searcher added to flush queue: "+pis.toString());
        boolean succ=flushQueue.offer(pis);
        if(!succ){
            logger.warn("Flush queue full!");
        }
        return succ;
    }

    // 消费对象，从队列中取走
    private PooledSearcher consume() throws InterruptedException {
        return flushQueue.take();
    }
}
