package eu.dnetlib.dhp.projecttoresult;

import java.io.Serializable;
import java.util.ArrayList;

public class ResultProjectSet implements Serializable {
    private String resultId;
    private ArrayList<String> projectSet;

    public String getResultId() {
        return resultId;
    }

    public void setResultId(String resultId) {
        this.resultId = resultId;
    }

    public ArrayList<String> getProjectSet() {
        return projectSet;
    }

    public void setProjectSet(ArrayList<String> project) {
        this.projectSet = project;
    }
}
