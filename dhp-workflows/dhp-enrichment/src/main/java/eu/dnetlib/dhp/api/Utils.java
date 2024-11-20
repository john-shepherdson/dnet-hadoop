
package eu.dnetlib.dhp.api;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

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
		c.setProviders(getRelevantDatasources(baseURL, communityId, sc.getSubCommunityId()));

		return c;
	}

	private static List<Provider> getRelevantDatasources(String baseURL, String communityId, String subcommunityId) {
		try {
		DatasourceList dl =  MAPPER
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

	private static void getRelatedOrganizations(String communityId, String baseURL, CommunityEntityMap communityEntityMap){

		try {
			List<String> associatedOrgs = MAPPER
					.readValue(
							QueryCommunityAPI.communityPropagationOrganization(communityId, baseURL), OrganizationList.class);
			associatedOrgs.forEach(o -> updateEntityMap(communityId, o, communityEntityMap, ModelSupport.getIdPrefix(Organization.class)));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}
	private static void getRelatedOrganizations(String communityId, String subcommunityId, String baseURL, CommunityEntityMap communityEntityMap){

		try {
			List<String> associatedOrgs = MAPPER
					.readValue(
							QueryCommunityAPI.subcommunityPropagationOrganization(communityId, subcommunityId, baseURL), OrganizationList.class);
			associatedOrgs.forEach(o -> updateEntityMap(communityId, o, communityEntityMap, ModelSupport.getIdPrefix(Organization.class)));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	private static void updateEntityMap(String communityId, String entityId, CommunityEntityMap communityEntityMap, String entityPrefix){

			if (!communityEntityMap
					.containsKey(entityPrefix + "|" + entityId))
                communityEntityMap.put(entityPrefix + "|" + entityId, new ArrayList<>());

		communityEntityMap.get(entityPrefix + "|" + entityId).add(communityId);

	}
	/**
	 * it returns for each organization the list of associated communities
	 */
	public static CommunityEntityMap getCommunityOrganization(String baseURL) throws IOException {
		CommunityEntityMap organizationMap = new CommunityEntityMap();
		List<CommunityModel> communityList = getValidCommunities(baseURL);
			communityList.forEach(community -> {
				getRelatedOrganizations(community.getId(), baseURL, organizationMap );
                try {
                    List<SubCommunityModel> subcommunities = getSubcommunities(community.getId(), baseURL);
					subcommunities.forEach(sc -> getRelatedOrganizations(community.getId(), sc.getSubCommunityId(), baseURL, organizationMap));
					} catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

		return organizationMap;
	}

	public static CommunityEntityMap getCommunityProjects(String baseURL) throws IOException {
		CommunityEntityMap projectMap = new CommunityEntityMap();

		getValidCommunities(baseURL)
			.forEach(community -> {
				addRelevantProjects(community.getId(), baseURL, projectMap);
				try {
					List<SubCommunityModel> subcommunities = getSubcommunities(community.getId(), baseURL);
					subcommunities.forEach(sc -> addRelevantProjects(community.getId(), sc.getSubCommunityId(), baseURL, projectMap));
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
		return projectMap;
	}

	private static void addRelevantProjects(
			String communityId,
			String baseURL,
			CommunityEntityMap communityEntityMap
	) {
		fetchAndProcessProjects(
				(page, size) -> {
                    try {
                        return QueryCommunityAPI.communityProjects(communityId, String.valueOf(page), String.valueOf(size), baseURL);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                },
				communityId,
				communityEntityMap
		);
	}

	private static void addRelevantProjects(
			String communityId,
			String subcommunityId,
			String baseURL,
			CommunityEntityMap communityEntityMap
	) {
		fetchAndProcessProjects(
				(page, size) -> {
                    try {
                        return QueryCommunityAPI.subcommunityProjects(communityId, subcommunityId, String.valueOf(page), String.valueOf(size), baseURL);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                },
				communityId,
				communityEntityMap
		);
	}

	@FunctionalInterface
	private interface ProjectQueryFunction {
		String query(int page, int size);
	}
	private static void fetchAndProcessProjects(
			ProjectQueryFunction projectQueryFunction,
			String communityId,
			CommunityEntityMap communityEntityMap
	) {
		int page = 0;
		final int size = 100;
		ContentModel contentModel;

		do {
			try {
				String response = projectQueryFunction.query(page, size);
				contentModel = MAPPER.readValue(response, ContentModel.class);

				if (!contentModel.getContent().isEmpty()) {
					contentModel.getContent().forEach(project ->
							updateEntityMap(
									communityId,
									project.getOpenaireId(),
									communityEntityMap,
									ModelSupport.getIdPrefix(Project.class)
							)
					);
				}
			} catch (IOException e) {
				throw new RuntimeException("Error processing projects for community: " + communityId, e);
			}
			page++;
		} while (!contentModel.getLast());
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
