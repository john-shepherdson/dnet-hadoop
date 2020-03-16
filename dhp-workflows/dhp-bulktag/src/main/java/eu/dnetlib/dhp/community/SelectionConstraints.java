package eu.dnetlib.dhp.community;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import eu.dnetlib.dhp.selectioncriteria.VerbResolver;


import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class SelectionConstraints implements Serializable {
    private List<Constraints> criteria;

    public SelectionConstraints() {
    }


    public List<Constraints> getCriteria() {
        return criteria;
    }

    public void setCriteria(List<Constraints> criteria) {
        this.criteria = criteria;
    }

    public void setSc(String json){
        Type collectionType = new TypeToken<Collection<Constraints>>(){}.getType();
        criteria = new Gson().fromJson(json, collectionType);
    }

    //Constraints in or
    public boolean verifyCriteria(final Map<String, List<String>> param){
        for(Constraints selc : criteria) {
            if(selc.verifyCriteria(param)){
                return true;
            }
        }
        return false;
    }
    public void setSelection(VerbResolver resolver) {

        for(Constraints cs : criteria){
            cs.setSelection(resolver);
        }
    }
}