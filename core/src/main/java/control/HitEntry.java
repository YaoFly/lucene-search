package control;

public class HitEntry{
    private double score=0;
    private String id="";
    private String tags="";
    private String keyword="";

    public void setScore(double sc){score=sc;}
    public double getScore(){return score;}

    public void setId(String d){id=d;}
    public String getId(){return id;}

    public void setTags(String t){tags=t;}
    public String getTags(){return tags;}

    public String getKeyword() {
        return keyword;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }
}
