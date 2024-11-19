
package eu.dnetlib.dhp.api;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;

import eu.dnetlib.dhp.api.model.*;
import eu.dnetlib.dhp.bulktag.community.Community;
import eu.dnetlib.dhp.bulktag.community.CommunityConfiguration;
import eu.dnetlib.dhp.bulktag.community.Provider;
import eu.dnetlib.dhp.bulktag.criteria.VerbResolver;
import eu.dnetlib.dhp.bulktag.criteria.VerbResolverFactory;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.Project;

/**
 * @author miriam.baglioni
 * @Date 09/10/23
 */
public class Utils implements Serializable {
	private static final ObjectMapper MAPPER = new ObjectMapper();
	private static final VerbResolver resolver = VerbResolverFactory.newInstance();

	public static CommunityConfiguration getCommunityConfiguration(String baseURL) throws IOException {
		final Map<String, Community> communities = Maps.newHashMap();
		List<CommunityModel> communityList = getValidCommunities(baseURL);
		List<Community> validCommunities = new ArrayList<>();
		communityList
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
					p.setOpenaireId(ModelSupport.getIdPrefix(Datasource.class) + "|" + d.getOpenaireId());
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

		//add subcommunities information if any
		communityList.forEach(community -> {
			try {
				List<SubCommunityModel> subcommunities = getSubcommunities(community.getId(), baseURL);
				subcommunities.forEach(sc ->
						validCommunities.add(getSubCommunityConfiguration(baseURL, community.getId(), sc)));
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

	private static @NotNull Community getSubCommunityConfiguration(String baseURL, String communityId, SubCommunityModel sc) {
		Community c = getCommunity(sc);
		c.setProviders(getSubcommunityDatasources(baseURL, communityId, sc.getSubCommunityId()));

		return c;
	}

	private static List<Provider> getSubcommunityDatasources(String baseURL, String communityId, String subcommunityId) {
		try {
		DatasourceList dl = null;
			dl = MAPPER
					.readValue(
							QueryCommunityAPI.subcommunityDatasource(communityId, subcommunityId, baseURL), DatasourceList.class);
			return dl.stream().map(d -> {
						if (d.getEnabled() == null || Boolean.FALSE.equals(d.getEnabled()))
							return null;
						Provider p = new Provider();
						p.setOpenaireId(ModelSupport.getIdPrefix(Datasource.class) + "|" + d.getOpenaireId());
						p.setSelectionConstraints(d.getSelectioncriteria());
						if (p.getSelectionConstraints() != null)
							p.getSelectionConstraints().setSelection(resolver);
						return p;
					})
					.filter(Objects::nonNull)
					.collect(Collectors.toList());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static <C extends CommonConfigurationModel> Community getCommonConfiguration(C input){
		Community c = new Community();
		c.setZenodoCommunities(input.getOtherZenodoCommunities());
		if (StringUtils.isNotBlank(input.getZenodoCommunity()))
			c.getZenodoCommunities().add(input.getZenodoCommunity());
		c.setSubjects(input.getSubjects());
		c.getSubjects().addAll(input.getFos());
		c.getSubjects().addAll(input.getSdg());
		if (input.getAdvancedConstraints() != null) {
			c.setConstraints(input.getAdvancedConstraints());
			c.getConstraints().setSelection(resolver);
		}
		if (input.getRemoveConstraints() != null) {
			c.setRemoveConstraints(input.getRemoveConstraints());
			c.getRemoveConstraints().setSelection(resolver);
		}
		return c;

	}

	private static Community getCommunity(SubCommunityModel sc) {
		Community c = getCommonConfiguration(sc);
		c.setId(sc.getSubCommunityId());
		return c;
	}



	private static Community getCommunity(CommunityModel cm) {
		Community c = getCommonConfiguration(cm);
		c.setId(cm.getId());

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

	public static List<SubCommunityModel> getSubcommunities(String communityId, String baseURL) throws IOException {
		return MAPPER.readValue(QueryCommunityAPI.subcommunities(communityId, baseURL), SubCommunitySummary.class);
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

	public static List<EntityCommunities> getDatasourceCommunities(String baseURL) throws IOException {
		List<CommunityModel> validCommunities = getValidCommunities(baseURL);
		HashMap<String, Set<String>> map = new HashMap<>();
		String entityPrefix = ModelSupport.getIdPrefix(Datasource.class) + "|";

		validCommunities.forEach(c -> {
			try {
				new ObjectMapper()
					.readValue(QueryCommunityAPI.communityDatasource(c.getId(), baseURL), DatasourceList.class)
					.forEach(d -> {
						if (!map.keySet().contains(d.getOpenaireId()))
							map.put(d.getOpenaireId(), new HashSet<>());

						map.get(d.getOpenaireId()).add(c.getId());
					});
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});

		List<EntityCommunities> temp = map
			.keySet()
			.stream()
			.map(k -> EntityCommunities.newInstance(entityPrefix + k, getCollect(k, map)))
			.collect(Collectors.toList());

		return temp;

	}

	@NotNull
	private static List<String> getCollect(String k, HashMap<String, Set<String>> map) {
		List<String> temp = map.get(k).stream().collect(Collectors.toList());
		return temp;
	}

}
