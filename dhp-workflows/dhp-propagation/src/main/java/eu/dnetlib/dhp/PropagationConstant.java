package eu.dnetlib.dhp;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class PropagationConstant {
    public static final String INSTITUTIONAL_REPO_TYPE = "pubsrepository::institutional";

    public final static String PROPAGATION_DATA_INFO_TYPE = "propagation";

    public static final String TRUE = "true";


    public final static String DNET_COUNTRY_SCHEMA = "dnet:countries";
    public final static String DNET_SCHEMA_NAME = "dnet:provenanceActions";
    public final static String DNET_SCHEMA_ID = "dnet:provenanceActions";

    public final static String PROPAGATION_COUNTRY_INSTREPO_CLASS_ID = "country:instrepos";
    public final static String PROPAGATION_COUNTRY_INSTREPO_CLASS_NAME = "Propagation of country to result collected from datasources of type institutional repositories";

    public final static String PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_ID = "result:organization:instrepo";
    public final static String PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_NAME = "Propagation of affiliation to result collected from datasources of type institutional repository";

    public final static String PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_ID = "result:project:semrel";
    public final static String PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_NAME = "Propagation of result to project through semantic relation";

    public final static String PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_ID = "result:community:semrel";
    public final static String PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_NAME = " Propagation of result belonging to community through semantic relation";

    public final static String PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_ID = "result:community:organization";
    public final static String PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_NAME = " Propagation of result belonging to community through organization";

    public final static String PROPAGATION_ORCID_TO_RESULT_FROM_SEM_REL_CLASS_ID = "propagation:orcid:result";
    public static final String PROPAGATION_ORCID_TO_RESULT_FROM_SEM_REL_CLASS_NAME = "Propagation of ORCID through result linked by isSupplementedBy or isSupplementTo semantic relations";

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


    public static final String RELATION_RESULT_REPRESENTATIVERESULT_REL_CLASS = "isMergedIn";
    public static final String RELATION_REPRESENTATIVERESULT_RESULT_CLASS = "merges";


    public static final String RELATION_ORGANIZATIONORGANIZATION_REL_TYPE = "organizationOrganization";

    public static final String RELATION_DEDUPORGANIZATION_SUBREL_TYPE = "dedup";

    public static final String PROPAGATION_AUTHOR_PID = "ORCID";

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
        di.setDeletedbyinference(false);
        di.setTrust("0.85");
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

    public static JavaPairRDD<String, TypedRow> getResultResultSemRel(List<String> allowedsemrel, JavaRDD<Relation> relations) {
        return relations
                .filter(r -> !r.getDataInfo().getDeletedbyinference())
                .filter(r -> allowedsemrel.contains(r.getRelClass()) && RELATION_RESULTRESULT_REL_TYPE.equals(r.getRelType()))
                .map(r -> {
                    TypedRow tr = new TypedRow();
                    tr.setSourceId(r.getSource());
                    tr.setTargetId(r.getTarget());
                    return tr;
                })
                .mapToPair(toPair());
    }


    public static String getConstraintList(String text, List<String> constraints){
        String ret = " and (" + text + constraints.get(0) + "'";
        for (int  i =1; i < constraints.size(); i++){
            ret += " OR " + text  + constraints.get(i) + "'";
        }
        ret += ")";
        return ret;
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
            TypedRow tr = new TypedRow();
            tr.setSourceId(dsId);
            tr.setTargetId(oaf.getId());
            tr.setType(type);
            lst.add(tr);
        }
        return lst;
    }

    public static void updateResultForCommunity(JavaPairRDD<String, Result> results, JavaPairRDD<String, TypedRow> toupdateresult, String outputPath, String type, String class_id, String class_name) {
        results.leftOuterJoin(toupdateresult)
                .map(p -> {
                    Result r = p._2()._1();
                    if (p._2()._2().isPresent()){
                        Set<String> communityList = p._2()._2().get().getAccumulator();
                        for(Context c: r.getContext()){
                            if (communityList.contains(c.getId())){
                                //verify if the datainfo for this context contains propagation
                                if (!c.getDataInfo().stream().map(di -> di.getInferenceprovenance()).collect(Collectors.toSet()).contains(PROPAGATION_DATA_INFO_TYPE)){
                                    c.getDataInfo().add(getDataInfo(PROPAGATION_DATA_INFO_TYPE, class_id, class_name));
                                    //community id already in the context of the result. Remove it from the set that has to be added
                                    communityList.remove(c.getId());
                                }
                            }
                        }
                        List<Context> cc = r.getContext();
                        for(String cId: communityList){
                            Context context = new Context();
                            context.setId(cId);
                            context.setDataInfo(Arrays.asList(getDataInfo(PROPAGATION_DATA_INFO_TYPE, class_id, class_name)));
                            cc.add(context);
                        }
                        r.setContext(cc);
                    }
                    return r;
                })
                .map(p -> new ObjectMapper().writeValueAsString(p))
                .saveAsTextFile(outputPath+"/"+type);
    }

    public static void createOutputDirs(String outputPath, FileSystem fs) throws IOException {
        if (fs.exists(new Path(outputPath))) {
            fs.delete(new Path(outputPath), true);
        }
        fs.mkdirs(new Path(outputPath));
    }

}
