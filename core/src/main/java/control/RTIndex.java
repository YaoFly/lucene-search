package control;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.lionsoul.jcseg.analyzer.JcsegAnalyzer;
import org.lionsoul.jcseg.tokenizer.core.JcsegTaskConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;


class RTIndex {
    class FlushControl {
        private final int READER_FLUSH_TIME=60;
        private final int INDEX_COMMIT_TIME=600;
        private final ScheduledExecutorService scheduler =
                Executors.newScheduledThreadPool(1);

        void flushForAWhile(RTIndex rti) {
            final Runnable flusher = rti::forceReopenReader;
            final ScheduledFuture<?> flushHandle =
                    scheduler.scheduleAtFixedRate(flusher, READER_FLUSH_TIME, READER_FLUSH_TIME, SECONDS);
            final Runnable commiter = rti::forceCommit;
            final ScheduledFuture<?> commitHandle =
                    scheduler.scheduleAtFixedRate(commiter, INDEX_COMMIT_TIME, INDEX_COMMIT_TIME, SECONDS);
        }

        void cancelAll(){
            try{
                scheduler.shutdown();
                if(!scheduler.awaitTermination(10, TimeUnit.SECONDS)){
                    scheduler.shutdownNow();
                }
            }catch(Exception e){
                logger.error("FlushControl shutdown error.");
            }
        }
    }

    private final static int MAX_COMMIT_COUNT=1000;
    private String indexPath="";
    private IndexReader indexReader=null;
    private IndexWriter indexWriter=null;
    private IndexSearcher indexSearcher=null;
    private int commitCount=0;

//    private BinaryDocValues docValues = null;
    private Bits liveDocs = null;

    private boolean noFlush=false;

    private final static Logger logger= LoggerFactory.getLogger(RTIndex.class);

    private FlushControl fc=null;

    RTIndex(String path){
        this.indexPath=path;
        init();
    }

    RTIndex(String path,boolean noFlush){
        this.indexPath=path;
        this.noFlush=noFlush;
        init();
    }

    private boolean init(){
        //create index reader/writer
        try {
            //index writer
            Analyzer analyzer = createAnalyzer();
            IndexWriterConfig conf = new IndexWriterConfig(analyzer);
            conf.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            indexWriter = new IndexWriter(FSDirectory.open(Paths.get(indexPath)), conf);

            //index reader
            indexReader= DirectoryReader.open(indexWriter,true,true);
            postReopen();
        }catch(Exception e){
            logger.error("Creating RTIndex failure.");
            return false;
        }

        if(!noFlush){
            fc = new FlushControl();
            fc.flushForAWhile(this);
        }
        return true;
    }

    IndexReader getIndexReader(){return indexReader;}
    IndexSearcher getIndexSearcher(){return indexSearcher;}
    String getIndexPath(){return indexPath;}
    int getNumDocs(){return indexReader.numDocs();}

    private IndexReader forceReopenReader(){
        try {
            indexReader = DirectoryReader.open(indexWriter, true,false);
            postReopen();
            //logger.debug("Force reader flush: "+this.toString());
        }catch(Exception e){
            logger.error("Force reopen reader error: ",e.getMessage());
        }
        return indexReader;
    }

    private void forceCommit(){
        if(commitCount>0){
            try {
                logger.debug("Force commit: "+this.toString());
                indexWriter.commit();
            }catch(Exception e){
                logger.error("Commit error "+this.toString());
            }
            commitCount=0;
            forceReopenReader();
        }
    }

    private IndexReader reopenReaderIfNeeded(){
        try {
            IndexReader newIR = DirectoryReader.openIfChanged((DirectoryReader)indexReader,indexWriter,true);
            if(null!=newIR){
                indexReader=newIR;
                postReopen();
                logger.debug("Reopen reader: "+this.toString());
            }
        }catch(Exception e){
            logger.error("Reopen reader error: "+e.getMessage());
        }
        return indexReader;
    }

    private synchronized void postReopen(){
        try {
            indexSearcher = new IndexSearcher(indexReader);
//            docValues = MultiDocValues.getBinaryValues(indexReader, GlobalDocumentBuilder.FIELD_NAME_CEDD);
            liveDocs = MultiFields.getLiveDocs(indexReader);
        }catch(Exception e){
            logger.error("Post reopen reader error: "+e.getMessage());
        }
    }

    boolean addFeature(String id,String keyword){
        if(id.length()<=0){
            return false;
        }
        return addFeatureBytes(id,keyword);
    }

    private boolean addFeatureBytes(String id,String keyword){
        if(id.length()<=0){
            return false;
        }

        Document doc = new Document();
        //add id
        doc.add(new StringField(Define.FIELD_NAME_IDENTIFIER, id, Field.Store.YES));

//        //add hash
//        if (globalFeature.getFeatureVector().length <= 3100){
//            int[] hashes = BitSampling.generateHashes(globalFeature.getFeatureVector());
//            Field hash = new TextField(DocumentBuilder.FIELD_NAME_CEDD + DocumentBuilder.HASH_FIELD_SUFFIX, SerializationUtils.arrayToString(hashes), Field.Store.YES);
//            doc.add(hash);
//        }else{
//            logger.error("Could not create hashes, feature vector too long: " +globalFeature.getFeatureVector().length);
//        }

        //add keyword
        doc.add(new TextField(Define.FIELD_NAME_KEYWORD,keyword, Field.Store.YES));

        try {
            indexWriter.updateDocument(new Term(Define.FIELD_NAME_IDENTIFIER,id),doc);
            logger.debug("Update index id: "+id);
            if(!SearchGlobal.getInstance().getWriterModel()) {
                indexWriter.forceMerge(1);
                reopenReaderIfNeeded();
            }
            commitCount+=1;
            if(commitCount>MAX_COMMIT_COUNT){
                logger.info("Commiting index over "+MAX_COMMIT_COUNT+" docs..."+this.toString());
                indexWriter.commit();
                commitCount=0;
            }
            return true;
        }catch(Exception e){
            logger.error("Failed to add index: "+id);
        }
        return false;
    }

    boolean deleteDocument(String id){
        if(indexWriter!=null) {
            try {
                indexWriter.deleteDocuments(new Term(Define.FIELD_NAME_IDENTIFIER, id));
                logger.debug("Delete index id: "+id);
                reopenReaderIfNeeded();
                return true;
            }catch(Exception e){
                logger.error("Error deleting document: "+id);
            }
        }
        return false;
    }

    void shutdown(){
        fc.cancelAll();
        logger.info("Commiting changes..."+this.toString());
        try {
            if (indexReader != null) {
                indexReader.close();
            }
            if (indexWriter != null) {
                indexWriter.commit();
                indexWriter.close();
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    boolean isDeleted(int innerID){
        return indexReader.hasDeletions() && !liveDocs.get(innerID);
    }

    int getInnerID(String id){
        int innerID=-1;
        TermQuery tq = new TermQuery(new Term(Define.FIELD_NAME_IDENTIFIER, id));
        try {
            TopDocs topDocs = indexSearcher.search(tq, 1);
            if (topDocs.totalHits > 0) {
                innerID=topDocs.scoreDocs[0].doc;
                if(isDeleted(innerID)){
                    innerID=-1;
                }
            } else {
                return -1;
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return innerID;
    }

    String[] getID(int innerID){
        String idName[]=null;
        try {
                idName=indexReader.document(innerID)
                    .getValues(Define.FIELD_NAME_IDENTIFIER);
        }catch(Exception e){
            e.printStackTrace();
        }
        return idName;
    }

    String getKeyword(int innerID){
        String keyword="";
        try {
            keyword = indexReader.document(innerID).getValues(Define.FIELD_NAME_KEYWORD)[0];
        } catch (IOException e) {
            e.printStackTrace();
        }
        return keyword;
    }

    static Analyzer createAnalyzer(){
        return new JcsegAnalyzer(JcsegTaskConfig.COMPLEX_MODE);
    }
}
