package eu.dnetlib.dhp.actionmanager.personentity;

import eu.dnetlib.dhp.collection.orcid.model.Work;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.Serializable;
import java.util.ArrayList;

public class WorkList implements Serializable {
    private ArrayList<Work> workArrayList;

    public ArrayList<Work> getWorkArrayList() {
        return workArrayList;
    }

    public void setWorkArrayList(ArrayList<Work> workArrayList) {
        this.workArrayList = workArrayList;
    }

    public WorkList() {
        workArrayList = new ArrayList<>();
    }
}
