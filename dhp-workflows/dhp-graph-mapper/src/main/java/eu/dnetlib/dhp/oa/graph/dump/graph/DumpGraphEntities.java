package eu.dnetlib.dhp.oa.graph.dump.graph;

import eu.dnetlib.dhp.oa.graph.dump.DumpProducts;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.oa.graph.dump.community.CommunityMap;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.dump.oaf.ControlledField;
import eu.dnetlib.dhp.schema.dump.oaf.Country;
import eu.dnetlib.dhp.schema.dump.oaf.KeyValue;
import eu.dnetlib.dhp.schema.dump.oaf.Qualifier;
import eu.dnetlib.dhp.schema.dump.oaf.graph.*;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

public class DumpGraph implements Serializable {


    public void run(Boolean isSparkSessionManaged,
                    String inputPath,
                    String outputPath,
                    Class<? extends OafEntity> inputClazz,
                    CommunityMap communityMap) {

        SparkConf conf = new SparkConf();
        switch (ModelSupport.idPrefixMap.get(inputClazz)){
            case "50":
                DumpProducts d = new DumpProducts();
                d.run(isSparkSessionManaged,inputPath,outputPath,communityMap, inputClazz, true);
                break;
            case "40":
                runWithSparkSession(
                        conf,
                        isSparkSessionManaged,
                        spark -> {
                            Utils.removeOutputDir(spark, outputPath);
                            projectMap(spark, inputPath, outputPath, inputClazz);

                        });
                break;
            case "20":
                runWithSparkSession(
                        conf,
                        isSparkSessionManaged,
                        spark -> {
                            Utils.removeOutputDir(spark, outputPath);
                            organizationMap(spark, inputPath, outputPath, inputClazz);

                        });
                break;
        }
    }

    private static <E extends OafEntity> void projectMap(SparkSession spark, String inputPath, String outputPath, Class<E> inputClazz) {
        Utils.readPath(spark, inputPath, inputClazz)
                .map(p -> mapProject((eu.dnetlib.dhp.schema.oaf.Project)p), Encoders.bean(Project.class))
                .write()
                .mode(SaveMode.Overwrite)
                .option("compression","gzip")
                .json(outputPath);
    }

    private static Project mapProject(eu.dnetlib.dhp.schema.oaf.Project p) {
        Project project = new Project();

        project.setCollectedfrom(Optional.ofNullable(p.getCollectedfrom())
                .map(cf -> cf.stream().map(coll -> coll.getValue()).collect(Collectors.toList()))
                .orElse(new ArrayList<>()));

        Optional.ofNullable(p.getId())
                .ifPresent(id -> project.setId(id));

        Optional.ofNullable(p.getWebsiteurl())
                .ifPresent(w -> project.setWebsiteurl(w.getValue()));

        Optional.ofNullable(p.getCode())
                .ifPresent(code -> project.setCode(code.getValue()));

        Optional.ofNullable(p.getAcronym())
                .ifPresent(acronynim -> project.setAcronym(acronynim.getValue()));

        Optional.ofNullable(p.getTitle())
                .ifPresent(title -> project.setTitle(title.getValue()));

        Optional.ofNullable(p.getStartdate())
                .ifPresent(sdate -> project.setStartdate(sdate.getValue()));

        Optional.ofNullable(p.getEnddate())
                .ifPresent(edate -> project.setEnddate(edate.getValue()));

        Optional.ofNullable(p.getCallidentifier())
                .ifPresent(cide -> project.setCallidentifier(cide.getValue()));

        Optional.ofNullable(p.getKeywords())
                .ifPresent(key -> project.setKeywords(key.getValue()));

        Optional.ofNullable(p.getDuration())
                .ifPresent(duration -> project.setDuration(duration.getValue()));

        Optional<Field<String>> omandate = Optional.ofNullable(p.getOamandatepublications());
        Optional<Field<String>> oecsc39 = Optional.ofNullable(p.getEcsc39());
        boolean mandate = false;
        if(omandate.isPresent()){
            if (!omandate.get().getValue().equals("N")){
                mandate = true;
            }
        }
        if(oecsc39.isPresent()){
            if(!oecsc39.get().getValue().equals("N")){
                mandate = true;
            }
        }

        project.setOpenaccessmandateforpublications(mandate);
        project.setOpenaccessmandatefordataset(false);

        Optional.ofNullable(p.getEcarticle29_3())
                .ifPresent(oamandate -> project.setOpenaccessmandatefordataset(oamandate.getValue().equals("Y")));

        project.setSubject(Optional.ofNullable(p.getSubjects())
                .map(subjs -> subjs.stream().map(s -> s.getValue()).collect(Collectors.toList()))
                .orElse(new ArrayList<>()));

        Optional.ofNullable(p.getSummary())
                .ifPresent(summary -> project.setSummary(summary.getValue()));

        Optional<Float> ofundedamount = Optional.ofNullable(p.getFundedamount());
        Optional<Field<String>> ocurrency = Optional.ofNullable(p.getCurrency());
        Optional<Float> ototalcost = Optional.ofNullable(p.getTotalcost());

        if(ocurrency.isPresent()){
            if(ofundedamount.isPresent()){
                if(ototalcost.isPresent()){
                    project.setGranted(Granted.newInstance(ocurrency.get().getValue(), ototalcost.get(), ofundedamount.get()));
                }else{
                    project.setGranted(Granted.newInstance(ocurrency.get().getValue(), ofundedamount.get()));
                }
            }
        }

        project.setProgramme(Optional.ofNullable(p.getProgramme())
                .map(programme -> programme.stream().map(pg -> Programme.newInstance(pg.getCode(), pg.getDescription()))
                        .collect(Collectors.toList()))
                .orElse(new ArrayList<>()));

        project.setFunding(Optional.ofNullable(p.getFundingtree())
                .map(value -> value.stream()
                        .map(fundingtree -> getFunder(fundingtree.getValue())).collect(Collectors.toList()))
                .orElse(new ArrayList<>()));
        return project;
    }

    public static Funder getFunder(String fundingtree){

        Funder f = new Funder();
        final Document doc;
        try {
            doc = new SAXReader().read(new StringReader(fundingtree));
            f.setShortName(((org.dom4j.Node) (doc.selectNodes("//funder/shortname").get(0))).getText());
            f.setName(((org.dom4j.Node) (doc.selectNodes("//funder/name").get(0))).getText());
            f.setJurisdiction(((org.dom4j.Node) (doc.selectNodes("//funder/jurisdiction").get(0))).getText());
            f.setId(((org.dom4j.Node) (doc.selectNodes("//funder/id").get(0))).getText());
            List<Levels> fundings = new ArrayList<>();
            int level = 0;
            List<org.dom4j.Node> nodes = doc.selectNodes("//funding_level_"+level);
            while(nodes.size() > 0 ) {
                for(org.dom4j.Node n: nodes) {
                    Levels funding_stream = new Levels();
                    funding_stream.setLevel(String.valueOf(level));
                    List node = n.selectNodes("./name");
                    funding_stream.setName(((org.dom4j.Node)node.get(0)).getText());
                    node = n.selectNodes("./id");
                    funding_stream.setId(((org.dom4j.Node)node.get(0)).getText());
                    node = n.selectNodes("./description");
                    funding_stream.setDescription(((Node)node.get(0)).getText());
                    fundings.add(funding_stream);

                }
                level += 1;
                nodes = doc.selectNodes("//funding_level_"+level);
            }
            if(fundings.size() > 0 ) {
                f.setFunding_levels(fundings);
            }

            return f;
        } catch (DocumentException e) {
            e.printStackTrace();
        }
        return f;

    }

    private static <E extends OafEntity> void organizationMap(SparkSession spark, String inputPath, String outputPath, Class<E> inputClazz) {
        Utils.readPath(spark, inputPath,inputClazz)
                .map(o -> mapOrganization((eu.dnetlib.dhp.schema.oaf.Organization)o), Encoders.bean(Organization.class))
                .write()
                .mode(SaveMode.Overwrite)
                .option("compression","gzip")
                .json(outputPath);
    }

    private static Organization mapOrganization(eu.dnetlib.dhp.schema.oaf.Organization org){
        Organization organization = new Organization();

        Optional.ofNullable(org.getLegalshortname())
                .ifPresent(value -> organization.setLegalshortname(value.getValue()));

        Optional.ofNullable(org.getLegalname())
                .ifPresent(value -> organization.setLegalname(value.getValue()));

        Optional.ofNullable(org.getWebsiteurl())
                .ifPresent(value -> organization.setWebsiteurl(value.getValue()));

        Optional.ofNullable(org.getAlternativeNames())
                .ifPresent(value -> organization.setAlternativenames(value.stream()
                        .map( v-> v.getValue()).collect(Collectors.toList())));

        Optional.ofNullable(org.getCountry())
                .ifPresent(value -> organization.setCountry(Qualifier.newInstance(value.getClassid(), value.getClassname())));

        Optional.ofNullable(org.getId())
                .ifPresent(value -> organization.setId(value));

        Optional.ofNullable(org.getPid())
                .ifPresent(value -> organization.setPid(
                        value.stream().map(p -> ControlledField.newInstance(p.getQualifier().getClassid(), p.getValue())).collect(Collectors.toList())
                ));

        organization.setCollectedfrom(Optional.ofNullable(org.getCollectedfrom())
                .map(value -> value.stream()
                .map(cf -> KeyValue.newInstance(cf.getKey(),cf.getValue())).collect(Collectors.toList()))
                .orElse(new ArrayList<>()));

        return organization;
    }
}
