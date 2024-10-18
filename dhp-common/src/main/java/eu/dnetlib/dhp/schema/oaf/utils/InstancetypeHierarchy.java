
package eu.dnetlib.dhp.schema.oaf.utils;

import java.util.BitSet;

import org.apache.commons.lang3.StringUtils;

public enum InstancetypeHierarchy {
	// @formatter:off
	Unknown("UNKNOWN"),

		OtherLiteratureType("Other literature type", 38, Unknown),
			ConferenceObject("Conference object", 4, OtherLiteratureType),
			Research("Research", 14, OtherLiteratureType),
				Presentation("Presentation", 50, Research),
				Article("Article", 1, Research),
					SoftwarePaper("Software Paper", 32, Article),
					DataPaper("Data Paper", 31, Article),
					Preprint("Preprint", 16, Article),
			Thesis("Thesis", 44, OtherLiteratureType),
				DoctoralThesis("Doctoral thesis", 6, Thesis),
				MasterThesis("Master thesis", 7, Thesis),
				BachelorThesis("Bachelor thesis", 8, Thesis),

			Report("Report", 17, OtherLiteratureType),
				InternalReport("Internal report", 11, Report),
				ExternalResearchReport("External research report", 9, Report),
				ProjectDeliverable("Project deliverable", 34, Report),
				ProjectMilestone("Project milestone", 35, Report),

			Book("Book", 2, OtherLiteratureType),
				PartOfBook("Part of book or chapter of book", 13, Book),

			ContributionFor("Contribution for newspaper or weekly magazine", 5, OtherLiteratureType),
			Newsletter("Newsletter",12, OtherLiteratureType),
			Review("Review", 15, OtherLiteratureType),
			Patent("Patent", 19, OtherLiteratureType),
			ProjectProposal("Project proposal", 36, OtherLiteratureType),
			Journal("Journal", 43, OtherLiteratureType),
			DataManagementPlan("Data Management Plan", 45, OtherLiteratureType),

		OtherDatasetType("Other dataset type", 39, Unknown),
			Dataset("Dataset", 21, OtherDatasetType),
				Collection("Collection", 22, Dataset),
				Film("Film", 24, Dataset),
				Image("Image", 25, Dataset),
				Sound("Sound", 30, Dataset),
				Audiovisual("Audiovisual", 33, Dataset),
				ClinicalTrial("Clinical Trial", 37, Dataset),
				Bioentity("Bioentity", 46, Dataset),
				Protein("Protein", 47, Dataset),

		OtherORPType("Other ORP type", 20, Unknown),
			Lecture("Lecture", 10, OtherORPType),
			Annotation("Annotation", 18, OtherORPType),
			Event("Event", 23, OtherORPType),
			InteractiveResource("InteractiveResource", 26, OtherORPType),
			Model("Model", 27, OtherORPType),
			PhysicalObject("PhysicalObject", 28, OtherORPType),
			VirtualAppliance("Virtual Appliance", 42, OtherORPType),
			ResearchObject("Research Object", 48, OtherORPType),


		OtherSoftwareType("Other software type", 40, Unknown),
			Software("Software", 29, OtherSoftwareType);
// @formatter:on

	InstancetypeHierarchy(String classname) {
		this.classname = classname;
		this.classid = 0;
	}

	InstancetypeHierarchy(String classname, int classid) {
		this.classname = classname;
		this.classid = classid;
		parents.set(classid);
	}

	InstancetypeHierarchy(String classname, int classid, InstancetypeHierarchy parent) {
		this(classname, classid);
		parents.or(parent.parents);
	}

	boolean isParentOf(InstancetypeHierarchy o) {
		return o.parents.get(this.classid);
	}

	private final String classname;
	private final int classid;
	private final BitSet parents = new BitSet();

	static InstancetypeHierarchy fromClassName(String classname) {
		if (StringUtils.isNotBlank(classname)) {
			for (InstancetypeHierarchy it : InstancetypeHierarchy.values()) {
				if (it.classname.equals(classname)) {
					return it;
				}
			}
		}
		return Unknown;
	}

	static InstancetypeHierarchy fromClassid(String classid) {
		int n = Integer.parseInt(classid, 10);

		for (InstancetypeHierarchy it : InstancetypeHierarchy.values()) {
			if (it.classid == n) {
				return it;
			}
		}

		return Unknown;
	}
}
