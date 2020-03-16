package eu.dnetlib.dhp.community;


import com.google.gson.Gson;

import eu.dnetlib.dhp.selectioncriteria.VerbResolver;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dom4j.Node;

/**
 * Created by miriam on 01/08/2018.
 */
public class Datasource {
    private static final Log log = LogFactory.getLog(Datasource.class);

    private String openaireId;

    private SelectionConstraints selectionConstraints;


    public SelectionConstraints getSelCriteria() {
        return selectionConstraints;
    }

    public SelectionConstraints getSelectionConstraints() {
        return selectionConstraints;
    }

    public void setSelectionConstraints(SelectionConstraints selectionConstraints) {
        this.selectionConstraints = selectionConstraints;
    }

    public void setSelCriteria(SelectionConstraints selCriteria) {
        this.selectionConstraints = selCriteria;
    }

    public String getOpenaireId() {
        return openaireId;
    }

    public void setOpenaireId(String openaireId) {
        this.openaireId = openaireId;
    }

    private void setSelCriteria(String json, VerbResolver resolver){
        log.info("Selection constraints for datasource = " + json);
        selectionConstraints = new Gson().fromJson(json, SelectionConstraints.class);

        selectionConstraints.setSelection(resolver);
    }

    public void setSelCriteria(Node n, VerbResolver resolver){
        try{
            setSelCriteria(n.getText(),resolver);
        }catch(Exception e) {
            log.info("not set selection criteria... ");
            selectionConstraints =null;
        }

    }



}