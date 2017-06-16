package control;


import java.util.Vector;

public class SearchConfig{
    private String indexPath="";
    private int maxResult=10;
    private int shards = 9;
    private boolean writerModel = false;
    private int port=9199;
    private Vector<String> workers;

    public void setIndexPath(String ip){indexPath=ip;}
    String getIndexPath(){return indexPath;}

    public void setMaxResult(int maxR){maxResult=maxR;}
    int getMaxResult(){return maxResult;}

    public void setPort(int p){port=p;}
    int getPort(){return port;}

    public void setWorkers(Vector<String> w){workers=w;}
    Vector<String> getWorkers(){return workers;}
    public void addWorker(String worker) {workers.add(worker);}

    int getShards() {return shards;}
    public void setShards(int shards) {this.shards = shards;}

    public boolean isWriterModel() {return writerModel;}

    public void setWriterModel(boolean writerModel) {this.writerModel = writerModel;}
}
