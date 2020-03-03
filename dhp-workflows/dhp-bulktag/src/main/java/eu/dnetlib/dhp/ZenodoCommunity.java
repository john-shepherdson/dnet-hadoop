package eu.dnetlib.dhp;

import com.google.gson.Gson;
import org.dom4j.Node;


/**
 * Created by miriam on 01/08/2018.
 */
public class ZenodoCommunity {

    private String zenodoCommunityId;

    private SelectionConstraints selCriteria;

    public String getZenodoCommunityId() {
        return zenodoCommunityId;
    }

    public void setZenodoCommunityId(String zenodoCommunityId) {
        this.zenodoCommunityId = zenodoCommunityId;
    }

    public SelectionConstraints getSelCriteria() {
        return selCriteria;
    }

    public void setSelCriteria(SelectionConstraints selCriteria) {
        this.selCriteria = selCriteria;
    }

    private void setSelCriteria(String json){
        //Type collectionType = new TypeToken<Collection<Constraints>>(){}.getType();
        selCriteria = new Gson().fromJson(json, SelectionConstraints.class);

    }

    public void setSelCriteria(Node n){
        if (n==null){
            selCriteria = null;
        }else{
            setSelCriteria(n.getText());
        }
    }

}