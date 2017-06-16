package control;


class SearchParam{
    private String id="";
    private int shard=-1;
    private String keyword="";

    public void setId(String i){id=i;}
    public String getId(){return id;}

    public void setShard(int i){shard=i;}
    public int getShard(){return shard;}

    public String getKeyword() {return keyword;}
    public void setKeyword(String keyword) {this.keyword = keyword;}
}
