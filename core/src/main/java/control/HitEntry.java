package control;

public class HitEntry{
    private double score=0;
    private String id="";
    private String tags="";
    private String keyword="";


    public void setId(String d){id=d;}
    public String getId(){return id;}

    public String getKeyword() {
        return keyword;
    }
    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }
}
