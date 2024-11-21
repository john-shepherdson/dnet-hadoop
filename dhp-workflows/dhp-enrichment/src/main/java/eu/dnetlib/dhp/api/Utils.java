
package eu.dnetlib.dhp.api;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
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

	@FunctionalInterface
	private interface ProjectQueryFunction {
		String query(int page, int size);
	}

	@FunctionalInterface
	private interface DatasourceQueryFunction{
		String query();
	}

	//PROJECT METHODS
	public static CommunityEntityMap getProjectCommunityMap(String baseURL) throws IOException {
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
					contentModel.getContent().forEach(project ->communityEntityMap.add(
							ModelSupport.getIdPrefix(Project.class) + "|" + project.getOpenaireId(), communityId)
					);
				}
			} catch (IOException e) {
				throw new RuntimeException("Error processing projects for community: " + communityId, e);
			}
			page++;
		} while (!contentModel.getLast());
	}

	private static List<Provider> getCommunityContentProviders(
			DatasourceQueryFunction datasourceQueryFunction
	) {
			try {
				String response = datasourceQueryFunction.query();
				DatasourceList datasourceList = MAPPER.readValue(response, DatasourceList.class);

				return datasourceList.stream().map(d -> {
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
				throw new RuntimeException("Error processing datasource information: " +  e);
			}

	}

	/**
	 * Select the communties with status different from hidden
	 * @param baseURL the base url of the API to be queried
	 * @return the list of communities in the CommunityModel class
	 * @throws IOException
	 */
	public static List<CommunityModel> getValidCommunities(String baseURL) throws IOException {
		List<CommunityModel> listCommunity = MAPPER
				.readValue(QueryCommunityAPI.communities(baseURL), new TypeReference<List<CommunityModel>>() {
				});
		return listCommunity.stream()
				.filter(
						community -> !community.getStatus().equals("hidden") &&
								(community.getType().equals("ri") || community.getType().equals("community")))
				.collect(Collectors.toList());
	}

	/**
	 * Sets the Community information from the replies of the communityAPIs
	 * @param baseURL the base url of the API to be queried
	 * @param communityModel the communityModel as replied by the APIs
	 * @return the community set with information from the community model and for the content providers
	 */
	private static Community getCommunity(String baseURL, CommunityModel communityModel) {
			Community community =  getCommunity(communityModel);
			community.setProviders(getCommunityContentProviders(()->{
				try {
					return QueryCommunityAPI.communityDatasource(community.getId(),baseURL);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}));

			return community;
	}

	/**
	 * extends the community configuration for the subcommunity by adding the content providers
	 * @param baseURL
	 * @param communityId
	 * @param sc
	 * @return
	 */
	private static @NotNull Community getSubCommunityConfiguration(String baseURL, String communityId, SubCommunityModel sc) {
		Community c = getCommunity(sc);
		c.setProviders(getCommunityContentProviders(()->{
			try {
				return QueryCommunityAPI.subcommunityDatasource(communityId, sc.getSubCommunityId(), baseURL);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}));

		return c;
	}

	/**
	 * Gets all the sub-comminities fir a given community identifier
	 * @param communityId
	 * @param baseURL
	 * @return
	 */
	private static List<Community> getSubCommunity(String communityId, String baseURL){
		try {
			List<SubCommunityModel> subcommunities = getSubcommunities(communityId, baseURL);
			return subcommunities.stream().map(sc ->
							getSubCommunityConfiguration(baseURL, communityId, sc))
					.collect(Collectors.toList());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * prepare the configuration for the communities and sub-communities
	 * @param baseURL
	 * @return
	 * @throws IOException
	 */
	public static CommunityConfiguration getCommunityConfiguration(String baseURL) throws IOException {
		final Map<String, Community> communities = Maps.newHashMap();
		List<CommunityModel> communityList = getValidCommunities(baseURL);
		List<Community> validCommunities = new ArrayList<>();
		communityList.forEach(community -> {
			validCommunities.add(getCommunity(baseURL, community));
			validCommunities.addAll(getSubCommunity(community.getId(), baseURL));
		});

		validCommunities.forEach(community -> {
			if (community.isValid())
				communities.put(community.getId(), community);
		});

		return new CommunityConfiguration(communities);
	}


	/**
	 * filles the common fields in the community model for both the communityconfiguration and the subcommunityconfiguration
	 * @param input
	 * @return
	 * @param <C>
	 */
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

	public static List<SubCommunityModel> getSubcommunities(String communityId, String baseURL) throws IOException {
		return MAPPER.readValue(QueryCommunityAPI.subcommunities(communityId, baseURL), new TypeReference<List<SubCommunityModel>>() {
		});
	}

	public static  CommunityEntityMap getOrganizationCommunityMap(String baseURL) throws IOException {
		return MAPPER.readValue(QueryCommunityAPI.propagationOrganizationCommunityMap(baseURL), CommunityEntityMap.class);
	}

	public static  CommunityEntityMap getDatasourceCommunityMap(String baseURL) throws IOException {
		return MAPPER.readValue(QueryCommunityAPI.propagationDatasourceCommunityMap(baseURL), CommunityEntityMap.class);
	}

	private static void getRelatedOrganizations(String communityId, String baseURL, CommunityEntityMap communityEntityMap){

		try {
			List<String> associatedOrgs = MAPPER
					.readValue(
							QueryCommunityAPI.communityPropagationOrganization(communityId, baseURL), EntityIdentifierList.class);
			associatedOrgs.forEach(o -> communityEntityMap.add(ModelSupport.getIdPrefix(Organization.class) + "|" + o, communityId ));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	private static void getRelatedOrganizations(String communityId, String subcommunityId, String baseURL, CommunityEntityMap communityEntityMap){

		try {
			List<String> associatedOrgs = MAPPER
					.readValue(
							QueryCommunityAPI.subcommunityPropagationOrganization(communityId, subcommunityId, baseURL), EntityIdentifierList.class);
			associatedOrgs.forEach(o -> communityEntityMap.add(ModelSupport.getIdPrefix(Organization.class) + "|" + o, communityId ));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

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


	public static List<String> getCommunityIdList(String baseURL) throws IOException {
		return getValidCommunities(baseURL)
			.stream()
				.flatMap(communityModel -> {
					List<String> communityIds = new ArrayList<>();
					communityIds.add(communityModel.getId());
                    try {
                        Utils.getSubcommunities(communityModel.getId(), baseURL).forEach(sc -> communityIds.add(sc.getSubCommunityId()));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return communityIds.stream();
				})

			.collect(Collectors.toList());
	}

	public static CommunityEntityMap getCommunityDatasource(String baseURL) throws IOException {
		CommunityEntityMap datasourceMap = new CommunityEntityMap();

		getValidCommunities(baseURL)
				.forEach(community -> {
					getRelatedDatasource(community.getId(), baseURL, datasourceMap);
					try {
						List<SubCommunityModel> subcommunities = getSubcommunities(community.getId(), baseURL);
						subcommunities.forEach(sc -> getRelatedDatasource(community.getId(), sc.getSubCommunityId(), baseURL, datasourceMap));
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				});
		return datasourceMap;
	}

	private static void getRelatedDatasource(String communityId, String baseURL, CommunityEntityMap communityEntityMap){

		try {
			List<String> associatedDatasources = MAPPER
					.readValue(
							QueryCommunityAPI.communityDatasource(communityId, baseURL), EntityIdentifierList.class);
			associatedDatasources.forEach(d -> communityEntityMap.add(ModelSupport.getIdPrefix(Datasource.class) + "|" + d, communityId ));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	private static void getRelatedDatasource(String communityId, String baseURL,String subcommunityId, CommunityEntityMap communityEntityMap){

		try {
			List<String> associatedDatasources = MAPPER
					.readValue(
							QueryCommunityAPI.subcommunityDatasource(communityId, subcommunityId, baseURL), EntityIdentifierList.class);
			associatedDatasources.forEach(d -> communityEntityMap.add(ModelSupport.getIdPrefix(Datasource.class) + "|" + d, communityId ));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}


}
