package eu.dnetlib.dhp.projecttoresult;

import java.io.Serializable;
import java.util.ArrayList;

public class ProjectResultSet implements Serializable {
    private String projectId;
    private ArrayList<String> resultSet;

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public ArrayList<String> getResultSet() {
        return resultSet;
    }

    public void setResultSet(ArrayList<String> resultSet) {
        this.resultSet = resultSet;
    }
}
