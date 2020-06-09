package eu.dnetlib.dhp.oa.graph.dump;

import eu.dnetlib.dhp.schema.dump.oaf.*;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Journal;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import scala.collection.immutable.Stream;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class Mapper implements Serializable {

    public static <I extends eu.dnetlib.dhp.schema.oaf.Result, O extends eu.dnetlib.dhp.schema.dump.oaf.Result> O map(
            I input, Map<String,String> communityMap){

        O out = null;
        switch (input.getResulttype().getClassid()){
            case "publication":
                out = (O)new Publication();
                Optional<Journal> journal = Optional.ofNullable(((eu.dnetlib.dhp.schema.oaf.Publication) input).getJournal());
                if(journal.isPresent()){
                    Journal j = journal.get();
                        Container c = new Container();
                        c.setConferencedate(j.getConferencedate());
                        c.setConferenceplace(j.getConferenceplace());
                        c.setEdition(j.getEdition());
                        c.setEp(j.getEp());
                        c.setIss(j.getIss());
                        c.setIssnLinking(j.getIssnLinking());
                        c.setIssnOnline(j.getIssnOnline());
                        c.setIssnPrinted(j.getIssnPrinted());
                        c.setName(j.getName());
                        c.setSp(j.getSp());
                        c.setVol(j.getVol());
                        out.setContainer(c);
                    }
                break;
            case "dataset":
                Dataset d = new Dataset();
                eu.dnetlib.dhp.schema.oaf.Dataset id = (eu.dnetlib.dhp.schema.oaf.Dataset)input;
                d.setSize(id.getSize().getValue());
                d.setVersion(id.getVersion().getValue());

                List<eu.dnetlib.dhp.schema.oaf.GeoLocation> igl = id.getGeolocation();
                d.setGeolocation(igl.stream()
                        .filter(Objects::nonNull)
                        .map(gli -> {
                            GeoLocation gl = new GeoLocation();
                            gl.setBox(gli.getBox());
                            gl.setPlace(gli.getPlace());
                            gl.setPoint(gli.getPoint());
                            return gl;
                        }).collect(Collectors.toList()));
                out = (O)d;

                break;
            case "software":
                Software s = new Software();
                eu.dnetlib.dhp.schema.oaf.Software is = (eu.dnetlib.dhp.schema.oaf.Software)input;
                s.setCodeRepositoryUrl(is.getCodeRepositoryUrl().getValue());
                s.setDocumentationUrl(is.getDocumentationUrl()
                        .stream()
                        .map(du -> du.getValue()).collect(Collectors.toList()));
                s.setProgrammingLanguage(is.getProgrammingLanguage().getClassid());

                out = (O) s;
                break;
            case "otherresearchproduct":
                OtherResearchProduct or = new OtherResearchProduct();
                eu.dnetlib.dhp.schema.oaf.OtherResearchProduct ir = (eu.dnetlib.dhp.schema.oaf.OtherResearchProduct)input;
                or.setContactgroup(ir.getContactgroup().stream().map(cg -> cg.getValue()).collect(Collectors.toList()));
                or.setContactperson(ir.getContactperson().stream().map(cp->cp.getValue()).collect(Collectors.toList()));
                or.setTool(ir.getTool().stream().map(t -> t.getValue()).collect(Collectors.toList()));
                out = (O) or;
                break;
        }
        out.setAuthor(input.getAuthor()
                .stream()
                .map(oa -> {
                    Author a = new Author();
                    a.setAffiliation(oa.getAffiliation().stream().map(aff -> aff.getValue()).collect(Collectors.toList()));
                    a.setFullname(oa.getFullname());
                    a.setName(oa.getName());
                    a.setSurname(oa.getSurname());
                    a.setRank(oa.getRank());
                    a.setPid(oa.getPid().stream().map(p -> {
                        ControlledField cf = new ControlledField();
                        cf.setScheme( p.getQualifier().getClassid());
                        cf.setValue( p.getValue());
                        return cf;
                    }).collect(Collectors.toList()));
                    return a;
                }).collect(Collectors.toList()));
        //I do not map Access Right UNKNOWN or OTHER
        if (Constants.accessRightsCoarMap.containsKey(input.getBestaccessright().getClassid())){
            AccessRight ar = new AccessRight();
            ar.setSchema(Constants.accessRightsCoarMap.get(input.getBestaccessright().getClassid()));
            ar.setCode(input.getBestaccessright().getClassid());
            ar.setLabel(input.getBestaccessright().getClassname());
            out.setBestaccessright(ar);
        }

        out.setCollectedfrom(input.getCollectedfrom().stream().map(cf -> KeyValue.newInstance(cf.getKey(), cf.getValue()))
                .collect(Collectors.toList()));

        Set<String> communities = communityMap.keySet();
        List<Context> contextList = input.getContext()
                .stream()
                .map(c -> {
                    if(communities.contains(c.getId())){
                        Context context = new Context();
                        context.setCode(c.getId());
                        context.setLabel(communityMap.get(c.getId()));
                        Optional<List<DataInfo>> dataInfo = Optional.ofNullable(c.getDataInfo());
                        if(dataInfo.isPresent()){
                           context.setProvenance(dataInfo.get().stream()
                                    .map(di -> {
                                        if (di.getInferred()){
                                            return di.getProvenanceaction().getClassid();
                                        }
                                        return null;
                                    }).filter(Objects::nonNull)
                                   .collect(Collectors.toList()));
                        }
                        return context;
                    }
                    return null;
                }
                ).filter(Objects::nonNull)
        .collect(Collectors.toList());
        if(contextList.size() > 0){
            out.setContext(contextList);
        }
        out.setContributor(input.getContributor()
                .stream()
                .map(c -> c.getValue()).collect(Collectors.toList()));
        out.setCountry(input.getCountry()
                .stream()
                .map(c -> {
                    Country country = new Country();
                    country.setCode(c.getClassid());
                    country.setLabel(c.getClassname());
                    Optional<DataInfo> dataInfo = Optional.ofNullable(c.getDataInfo());
                    if(dataInfo.isPresent()){
                        country.setProvenance(dataInfo.get().getProvenanceaction().getClassid());
                    }
                    return country;
                }).collect(Collectors.toList()));
        out.setCoverage(input.getCoverage().stream().map(c->c.getValue()).collect(Collectors.toList()));

        out.setDateofcollection(input.getDateofcollection());
        out.setDescription(input.getDescription().stream().map(d->d.getValue()).collect(Collectors.toList()));
        out.setEmbargoenddate(input.getEmbargoenddate().getValue());
        out.setFormat(input.getFormat().stream().map(f->f.getValue()).collect(Collectors.toList()));
        out.setId(input.getId());
        out.setOriginalId(input.getOriginalId());
        out.setInstance(input.getInstance()
                .stream()
                .map(i -> {
                    Instance instance = new Instance();
                    AccessRight ar = new AccessRight();
                    ar.setCode(i.getAccessright().getClassid());
                    ar.setLabel(i.getAccessright().getClassname());
                    if(Constants.accessRightsCoarMap.containsKey(i.getAccessright().getClassid())){
                        ar.setSchema(Constants.accessRightsCoarMap.get(i.getAccessright().getClassid()));
                    }
                    instance.setAccessright(ar);
                    instance.setCollectedfrom(KeyValue.newInstance(i.getCollectedfrom().getKey(), i.getCollectedfrom().getValue()));
                    instance.setHostedby(KeyValue.newInstance(i.getHostedby().getKey(),i.getHostedby().getValue()));
                    instance.setLicense(i.getLicense().getValue());
                    instance.setPublicationdata(i.getDateofacceptance().getValue());
                    instance.setRefereed(i.getRefereed().getValue());
                    instance.setType(i.getInstancetype().getClassid());
                    instance.setUrl(i.getUrl());
                    return instance;
                }).collect(Collectors.toList()));

        out.setLanguage(Qualifier.newInstance(input.getLanguage().getClassid(), input.getLanguage().getClassname()));
        out.setLastupdatetimestamp(input.getLastupdatetimestamp());

        Optional<List<StructuredProperty>> otitle = Optional.ofNullable(input.getTitle());
        if(otitle.isPresent()){
            List<StructuredProperty> iTitle = otitle.get()
                    .stream()
                    .filter(t -> t.getQualifier().getClassid().equalsIgnoreCase("main title"))
                    .collect(Collectors.toList());
            if(iTitle.size() > 0 ){
                out.setMaintitle(iTitle.get(0).getValue());
            }

            iTitle = otitle.get()
                    .stream()
                    .filter(t -> t.getQualifier().getClassid().equalsIgnoreCase("subtitle"))
                    .collect(Collectors.toList());
            if(iTitle.size() > 0){
                out.setSubtitle(iTitle.get(0).getValue());
            }

        }

        out.setPid(input.getPid().stream().map(p -> {
            ControlledField pid = new ControlledField();
            pid.setScheme(p.getQualifier().getClassid());
            pid.setValue(p.getValue());
            return pid;
        }).collect(Collectors.toList()));
        out.setPublicationdata(input.getDateofacceptance().getValue());
        out.setPublisher(input.getPublisher().getValue());
        out.setSource(input.getSource().stream().map(s -> s.getValue()).collect(Collectors.toList()));
        out.setSubject(input.getSubject().stream().map(s->{
            ControlledField subject = new ControlledField();
            subject.setScheme(s.getQualifier().getClassid());
            subject.setValue(s.getValue());
            return subject;
        }).collect(Collectors.toList()));
        out.setType(input.getResulttype().getClassid());

        return out;
    }

}
