package eu.dnetlib.dhp.actionmanager.bipfinder;

import java.io.Serializable;
import java.util.List;

public class BipScore implements Serializable {
    private String id;
    private List<Score> scoreList;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<Score> getScoreList() {
        return scoreList;
    }

    public void setScoreList(List<Score> scoreList) {
        this.scoreList = scoreList;
    }
}
