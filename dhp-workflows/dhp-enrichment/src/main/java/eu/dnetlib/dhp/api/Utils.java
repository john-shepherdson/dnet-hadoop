package eu.dnetlib.dhp.api;

import com.amazonaws.util.StringUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import eu.dnetlib.dhp.api.model.*;
import eu.dnetlib.dhp.bulktag.community.Community;
import eu.dnetlib.dhp.bulktag.community.CommunityConfiguration;
import eu.dnetlib.dhp.bulktag.community.Provider;
import eu.dnetlib.dhp.bulktag.criteria.VerbResolver;
import eu.dnetlib.dhp.bulktag.criteria.VerbResolverFactory;

import javax.management.Query;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author miriam.baglioni
 * @Date 09/10/23
 */
public class Utils implements Serializable {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final VerbResolver resolver = VerbResolverFactory.newInstance();

    public static CommunityConfiguration getCommunityConfiguration() throws IOException {
        final Map<String, Community> communities = Maps.newHashMap();
        List<Community> validCommunities = new ArrayList<>();
        getValidCommunities()
                .forEach(community -> {
                            try {
                                CommunityModel cm = MAPPER.readValue(QueryCommunityAPI.community(community.getId()), CommunityModel.class);
                                validCommunities.add(getCommunity(cm));
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                });
        validCommunities.forEach(community ->{
            try {
                DatasourceList dl = MAPPER.readValue(QueryCommunityAPI.communityDatasource(community.getId()), DatasourceList.class);
                community.setProviders(dl.stream().map(d -> {
//                    if(d.getEnabled() == null || Boolean.FALSE.equals(d.getEnabled()))
//                        return null;
                            Provider p = new Provider();
                            p.setOpenaireId("10|" + d.getOpenaireId());
                            p.setSelectionConstraints(d.getSelectioncriteria());
                            if(p.getSelectionConstraints() != null)
                                p.getSelectionConstraints().setSelection(resolver);
                            return p;
                        })
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        validCommunities.forEach(community ->{
            if(community.isValid())
                communities.put(community.getId(), community);
        });
        return new CommunityConfiguration(communities);
    }

    private static  Community getCommunity(CommunityModel cm){
        Community c = new Community();
        c.setId(cm.getId());
        c.setZenodoCommunities(cm.getOtherZenodoCommunities());
        if(!StringUtils.isNullOrEmpty(cm.getZenodoCommunity()))
            c.getZenodoCommunities().add(cm.getZenodoCommunity());
        c.setSubjects(cm.getSubjects());
        c.getSubjects().addAll(cm.getFos());
        c.getSubjects().addAll(cm.getSdg());
        c.setConstraints(cm.getAdvancedConstraints());
        if(c.getConstraints()!=null)
            c.getConstraints().setSelection(resolver);
        c.setRemoveConstraints(cm.getRemoveConstraints());
        if(c.getRemoveConstraints()!=null)
            c.getRemoveConstraints().setSelection(resolver);
        return c;
    }

    public static List<CommunityModel> getValidCommunities() throws IOException {
        return MAPPER.readValue(QueryCommunityAPI.communities(), CommunitySummary.class)
                .stream()
                .filter(community -> !community.getStatus().equals("hidden") &&
                        (community.getType().equals("ri") || community.getType().equals("community")))
                .collect(Collectors.toList());
    }
    public static CommunityEntityMap getCommunityOrganization() throws IOException {
        CommunityEntityMap organizationMap = new CommunityEntityMap();
        getValidCommunities()
                .forEach(community -> {
                    String id = community.getId();
                    try {
                        List<String> associatedOrgs = MAPPER.readValue(QueryCommunityAPI.communityPropagationOrganization(id), OrganizationList.class);
                        if(associatedOrgs.size() >0){
                            organizationMap.put(id, associatedOrgs);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
        return organizationMap;
    }

    public static CommunityEntityMap getCommunityProjects()throws IOException{
        CommunityEntityMap projectMap = new CommunityEntityMap();
        getValidCommunities()
                .forEach(community ->{
                        int page = -1;
                        int size = 100;
                    ContentModel cm = new ContentModel();
                    List<String> projectList = new ArrayList<>();
                do{
                    page ++;
                    try {
                       cm = MAPPER.readValue( QueryCommunityAPI.communityProjects(community.getId(), String.valueOf(page), String.valueOf(size)), ContentModel.class);
                       if (cm.getContent().size() > 0){

                           cm.getContent().forEach(p ->
                                   projectList.add ("40|" + p.getOpenaireId()));
                           projectMap.put(community.getId(), projectList);
                       }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }while (!cm.getLast());
    });
        return projectMap;
    }
}
