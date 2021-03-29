
package eu.dnetlib.dhp.oa.graph.raw.common;

//enum to specify the different actions available for the MigrateDbEntitiesApplication job
public enum MigrateAction {
	claims, // migrate claims to the raw graph
	openorgs_dedup, // migrate organizations from openorgs to the raw graph
	openorgs, // migrate organization from openorgs to the raw graph for provision
	openaire, // migrate openaire entities to the raw graph
	openaire_organizations // migrate openaire organizations entities to the raw graph
}
