Joins the graph nodes by resolving the links of distance = 1 to create an adjacency list of linked objects. The
operation considers all the entity types (publication, dataset, software, ORP, project, datasource, organization, and
all the possible relationships (similarity links produced by the Dedup process are excluded).

The operation is implemented by sequentially joining one entity type at time (E) with the relationships (R), and
again by E, finally grouped by E.id;

The workflow is organized in different parts aimed to to reduce the complexity of the operation 

1) PrepareRelationsJob: only consider relationships that are not virtually deleted ($.dataInfo.deletedbyinference ==
false), each entity can be linked at most to 100 other objects

2) CreateRelatedEntitiesJob: (phase 1): prepare tuples [relation - target entity] (R - T): for each entity type
E_i map E_i as RelatedEntity T_i to simplify the model and extracting only the necessary information join (R.target =
T_i.id) save the tuples (R_i, T_i) (phase 2): create the union of all the entity types E, hash by id read the tuples
(R, T), hash by R.source join E.id = (R, T).source, where E becomes the Source Entity S save the tuples (S, R, T)

3) AdjacencyListBuilderJob: given the tuple (S - R - T) we need to group by S.id -> List [ R - T ], mapping the
result as JoinedEntity

4) XmlConverterJob: convert the JoinedEntities as XML records
