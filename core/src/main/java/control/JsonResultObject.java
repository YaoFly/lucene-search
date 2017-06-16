package control;


import java.util.ArrayList;
import java.util.List;

public class JsonResultObject{
    private long took=0;
    private long hits=0;
    private List<HitEntry> results=new ArrayList<>();
    public void setTook(int t){took=t;}
    public long getTook(){return took;}
    public void setHits(int h){hits=h;}
    public long getHits(){return hits;}
    public void setResults(ArrayList<HitEntry> r){results=r;}
    public List<HitEntry> getResults(){return results;}
    public void addResult(HitEntry entry) {results.add(entry);}
}
