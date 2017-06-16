package control;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.analyzing.AnalyzingQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;


class RTSearcher {
    private RTIndex rtindex;
    private IndexReader reader = null;
    private IndexSearcher searcher = null;

    private int maxHits = 50;
    private final int MAX_TOP_N = 200000;
    //    private TreeSet<SimpleResult> docs = new TreeSet<>();
    private Analyzer analyzer = RTIndex.createAnalyzer();

    RTSearcher(int maxHits, RTIndex rti) {
        this.maxHits = maxHits;
        this.rtindex = rti;
        init();
    }

    private void init() {

        // for dirty check,we must save a pointer to index reader now
        this.reader = rtindex.getIndexReader();
        this.searcher = rtindex.getIndexSearcher();
    }

    boolean checkDirty() {
        return reader != rtindex.getIndexReader();
    }

    public ScoreDoc[] search(String keyword) throws ParseException, IOException {
        // find doc subset
        ScoreDoc[] hits;
        QueryParser qp = new AnalyzingQueryParser(Define.FIELD_NAME_KEYWORD, analyzer);
        qp.setAllowLeadingWildcard(true);
        qp.setAutoGeneratePhraseQueries(true);
        Query q = qp.parse(keyword);
        IndexSearcher is = rtindex.getIndexSearcher();
        TopDocs results = is.search(q, MAX_TOP_N);
        return results.scoreDocs;
    }

    public IndexSearcher getSearcher() {
        return searcher;
    }
}
