package eu.dnetlib.dhp;

import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PropagationConstant {
    public static final String INSTITUTIONAL_REPO_TYPE = "pubsrepository::institutional";

    public final static String PROPAGATION_DATA_INFO_TYPE = "propagation";


    public final static String DNET_COUNTRY_SCHEMA = "dnet:countries";
    public final static String DNET_SCHEMA_NAME = "dnet:provenanceActions";
    public final static String DNET_SCHEMA_ID = "dnet:provenanceActions";

    public final static String PROPAGATION_COUNTRY_INSTREPO_CLASS_ID = "country:instrepos";
    public final static String PROPAGATION_COUNTRY_INSTREPO_CLASS_NAME = "Propagation of country to result collected from datasources of type institutional repositories";
    public final static String PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_ID = "result:organization:instrepo";
    public final static String PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_NAME = "Propagation of affiliation to result collected from datasources of type institutional repository";
    public final static String PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_ID = "result:project:semrel";
    public final static String PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_NAME = "Propagation of result to project through semantic relation";


    public final static String RELATION_DATASOURCEORGANIZATION_REL_TYPE = "datasourceOrganization";
    public final static String RELATION_DATASOURCEORGANIZATION_SUBREL_TYPE = "provision";
    public final static String RELATION_ORGANIZATION_DATASOURCE_REL_CLASS = "isProvidedBy";
    public final static String RELATION_DATASOURCE_ORGANIZATION_REL_CLASS = "provides";

    public final static String RELATION_RESULTORGANIZATION_REL_TYPE = "resultOrganization";
    public final static String RELATION_RESULTORGANIZATION_SUBREL_TYPE = "affiliation";
    public final static String RELATION_ORGANIZATION_RESULT_REL_CLASS = "isAuthorInstitutionOf";
    public final static String RELATION_RESULT_ORGANIZATION_REL_CLASS = "hasAuthorInstitution";

    public static final String RELATION_RESULTRESULT_REL_TYPE = "resultResult";
    public static final String RELATION_RESULTRESULT_SUBREL_TYPE = "supplement";

    public static final String RELATION_RESULTPROJECT_REL_TYPE = "resultProject";
    public static final String RELATION_RESULTPROJECT_SUBREL_TYPE = "outcome";
    public static final String RELATION_RESULT_PROJECT_REL_CLASS = "isProducedBy";
    public static final String RELATION_PROJECT_RESULT_REL_CLASS = "produces";


    public static Country getCountry(String country){
        Country nc = new Country();
        nc.setClassid(country);
        nc.setClassname(country);
        nc.setSchemename(DNET_COUNTRY_SCHEMA);
        nc.setSchemeid(DNET_COUNTRY_SCHEMA);
        nc.setDataInfo(getDataInfo(PROPAGATION_DATA_INFO_TYPE, PROPAGATION_COUNTRY_INSTREPO_CLASS_ID, PROPAGATION_COUNTRY_INSTREPO_CLASS_NAME));
        return nc;
    }

    public static DataInfo getDataInfo(String inference_provenance, String inference_class_id, String inference_class_name){
        DataInfo di = new DataInfo();
        di.setInferred(true);
        di.setInferenceprovenance(inference_provenance);
        di.setProvenanceaction(getQualifier(inference_class_id, inference_class_name));
        return di;
    }

    public static Qualifier getQualifier(String inference_class_id, String inference_class_name) {
        Qualifier pa = new Qualifier();
        pa.setClassid(inference_class_id);
        pa.setClassname(inference_class_name);
        pa.setSchemeid(DNET_SCHEMA_ID);
        pa.setSchemename(DNET_SCHEMA_NAME);
        return pa;
    }


    public static Relation getRelation(String source, String target, String rel_class, String rel_type, String subrel_type, String inference_provenance, String inference_class_id, String inference_class_name){
        Relation r = new Relation();
        r.setSource(source);
        r.setTarget(target);
        r.setRelClass(rel_class);
        r.setRelType(rel_type);
        r.setSubRelType(subrel_type);
        r.setDataInfo(getDataInfo(inference_provenance, inference_class_id, inference_class_name));
        return r;
}

    public static PairFunction<TypedRow, String, TypedRow> toPair() {
        return e -> new Tuple2<>( e.getSourceId(), e);

    }

    public static List<TypedRow> getTypedRowsDatasourceResult(OafEntity oaf) {
        List<TypedRow> lst = new ArrayList<>();
        Set<String> datasources_provenance = new HashSet<>();
        List<Instance> instanceList = null;
        String type = "";
        if (oaf.getClass() == Publication.class) {
            instanceList = ((Publication) oaf).getInstance();
            type = "publication";
        }
        if (oaf.getClass() == Dataset.class){
            instanceList = ((Dataset)oaf).getInstance();
            type = "dataset";
        }

        if (oaf.getClass() == Software.class){
            instanceList = ((Software)oaf).getInstance();
            type = "software";
        }

        if (oaf.getClass() == OtherResearchProduct.class){
            instanceList = ((OtherResearchProduct)oaf).getInstance();
            type = "otherresearchproduct";
        }


        for (Instance i : instanceList) {
            datasources_provenance.add(i.getCollectedfrom().getKey());
            datasources_provenance.add(i.getHostedby().getKey());
        }
        for (String dsId : datasources_provenance) {
            lst.add(new TypedRow().setSourceId(dsId).setTargetId(oaf.getId()).setType(type));
        }
        return lst;
    }
}
