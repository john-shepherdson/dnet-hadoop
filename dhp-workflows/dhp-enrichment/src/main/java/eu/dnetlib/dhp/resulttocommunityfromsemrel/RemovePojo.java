package eu.dnetlib.dhp.resulttocommunityfromsemrel;

import java.io.Serializable;
import java.util.List;

public class RemovePojo implements Serializable {
    String resultId;
    List<String> contextList;

    public String getResultId() {
        return resultId;
    }

    public void setResultId(String resultId) {
        this.resultId = resultId;
    }

    public List<String> getContextList() {
        return contextList;
    }

    public void setContextList(List<String> contextList) {
        this.contextList = contextList;
    }

    public RemovePojo(String resultId, List<String> contextList) {
        this.resultId = resultId;
        this.contextList = contextList;
    }

    public RemovePojo() {

    }
}