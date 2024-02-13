
package eu.dnetlib.dhp.api;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.Project;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.util.StringUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;

import eu.dnetlib.dhp.api.model.*;
import eu.dnetlib.dhp.bulktag.community.Community;
import eu.dnetlib.dhp.bulktag.community.CommunityConfiguration;
import eu.dnetlib.dhp.bulktag.community.Provider;
import eu.dnetlib.dhp.bulktag.criteria.VerbResolver;
import eu.dnetlib.dhp.bulktag.criteria.VerbResolverFactory;

/**
 * @author miriam.baglioni
 * @Date 09/10/23
 */
public class Utils implements Serializable {
	private static final ObjectMapper MAPPER = new ObjectMapper();
	private static final VerbResolver resolver = VerbResolverFactory.newInstance();

	private static final Logger log = LoggerFactory.getLogger(Utils.class);

	public static CommunityConfiguration getCommunityConfiguration(String baseURL) throws IOException {
		final Map<String, Community> communities = Maps.newHashMap();
		List<Community> validCommunities = new ArrayList<>();
		getValidCommunities(baseURL)
			.forEach(community -> {
				try {
					CommunityModel cm = MAPPER
						.readValue(QueryCommunityAPI.community(community.getId(), baseURL), CommunityModel.class);
					validCommunities.add(getCommunity(cm));
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
		validCommunities.forEach(community -> {
			try {
				DatasourceList dl = MAPPER
					.readValue(
						QueryCommunityAPI.communityDatasource(community.getId(), baseURL), DatasourceList.class);
				community.setProviders(dl.stream().map(d -> {
					if (d.getEnabled() == null || Boolean.FALSE.equals(d.getEnabled()))
						return null;
					Provider p = new Provider();
					p.setOpenaireId(ModelSupport.getIdPrefix(Datasource.class)+"|" + d.getOpenaireId());
					p.setSelectionConstraints(d.getSelectioncriteria());
					if (p.getSelectionConstraints() != null)
						p.getSelectionConstraints().setSelection(resolver);
					return p;
				})
					.filter(Objects::nonNull)
					.collect(Collectors.toList()));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});

		validCommunities.forEach(community -> {
			if (community.isValid())
				communities.put(community.getId(), community);
		});
		return new CommunityConfiguration(communities);
	}

	private static Community getCommunity(CommunityModel cm) {
		Community c = new Community();
		c.setId(cm.getId());
		c.setZenodoCommunities(cm.getOtherZenodoCommunities());
		if (!StringUtils.isNullOrEmpty(cm.getZenodoCommunity()))
			c.getZenodoCommunities().add(cm.getZenodoCommunity());
		c.setSubjects(cm.getSubjects());
		c.getSubjects().addAll(cm.getFos());
		c.getSubjects().addAll(cm.getSdg());
		if (cm.getAdvancedConstraints() != null) {
			c.setConstraints(cm.getAdvancedConstraints());
			c.getConstraints().setSelection(resolver);
		}
		if (cm.getRemoveConstraints() != null) {
			c.setRemoveConstraints(cm.getRemoveConstraints());
			c.getRemoveConstraints().setSelection(resolver);
		}
		return c;
	}

	public static List<CommunityModel> getValidCommunities(String baseURL) throws IOException {
		return MAPPER
			.readValue(QueryCommunityAPI.communities(baseURL), CommunitySummary.class)
			.stream()
			.filter(
				community -> !community.getStatus().equals("hidden") &&
					(community.getType().equals("ri") || community.getType().equals("community")))
			.collect(Collectors.toList());
	}

	/**
	 * it returns for each organization the list of associated communities
	 */
	public static CommunityEntityMap getCommunityOrganization(String baseURL) throws IOException {
		CommunityEntityMap organizationMap = new CommunityEntityMap();
		String entityPrefix = ModelSupport.getIdPrefix(Organization.class);
		getValidCommunities(baseURL)
			.forEach(community -> {
				String id = community.getId();
				try {
					List<String> associatedOrgs = MAPPER
						.readValue(
							QueryCommunityAPI.communityPropagationOrganization(id, baseURL), OrganizationList.class);
					associatedOrgs.forEach(o -> {
						if (!organizationMap
							.keySet()
							.contains(
								entityPrefix + "|" + o))
							organizationMap.put(entityPrefix + "|" + o, new ArrayList<>());
						organizationMap.get(entityPrefix + "|" + o).add(community.getId());
					});
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});

		return organizationMap;
	}

	public static CommunityEntityMap getCommunityProjects(String baseURL) throws IOException {
		CommunityEntityMap projectMap = new CommunityEntityMap();
		String entityPrefix = ModelSupport.getIdPrefix(Project.class);
		getValidCommunities(baseURL)
			.forEach(community -> {
				int page = -1;
				int size = 100;
				ContentModel cm = new ContentModel();
				do {
					page++;
					try {
						cm = MAPPER
							.readValue(
								QueryCommunityAPI
									.communityProjects(
										community.getId(), String.valueOf(page), String.valueOf(size), baseURL),
								ContentModel.class);
						if (cm.getContent().size() > 0) {
							cm.getContent().forEach(p -> {
								if (!projectMap.keySet().contains(entityPrefix + "|" + p.getOpenaireId()))
									projectMap.put(entityPrefix + "|" + p.getOpenaireId(), new ArrayList<>());
								projectMap.get(entityPrefix + "|" + p.getOpenaireId()).add(community.getId());
							});
						}
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				} while (!cm.getLast());
			});
		return projectMap;
	}

	public static List<String> getCommunityIdList(String baseURL) throws IOException {
		return getValidCommunities(baseURL)
			.stream()
			.map(community -> community.getId())
			.collect(Collectors.toList());
	}

	public static List<EntityCommunities> getDatasourceCommunities(String baseURL)throws IOException{
		List<CommunityModel> validCommunities = getValidCommunities(baseURL);
		HashMap<String, Set<String>> map = new HashMap<>();
		validCommunities.forEach(c -> {
			try {
				new ObjectMapper().readValue(QueryCommunityAPI.communityDatasource(c.getId(), baseURL), DatasourceList.class)
						.forEach(d -> {
							if (!map.keySet().contains(d.getOpenaireId()))
								map.put(d.getOpenaireId(), new HashSet<>());

							map.get(d.getOpenaireId()).add(c.getId());
						});
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});


		return map.keySet().stream().map(k -> EntityCommunities.newInstance(k, map.get(k).stream().collect(Collectors.toList()))).collect(Collectors.toList());

	}


}
