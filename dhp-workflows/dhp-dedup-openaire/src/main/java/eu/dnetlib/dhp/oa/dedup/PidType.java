package eu.dnetlib.dhp.oa.dedup;

public enum PidType {

    //from the less to the more important
    undefined,
    orcid,
    ror,
    grid,
    pdb,
    arXiv,
    pmid,
    doi;

    public static PidType classidValueOf(String s){
        try {
            return PidType.valueOf(s);
        }
        catch (Exception e) {
            return PidType.undefined;
        }
    }

}

//dnet:pid_types
//"actrn"
//"nct"
//"euctr"
//"epo_id"
//"gsk"
//"GeoPass"
//"GBIF"
//"isrctn"
//"ISNI"
//"jprn"
//"mag_id"
//"oai"
//"orcid"
//"PANGAEA"
//"epo_nr_epodoc"
//"UNKNOWN"
//"VIAF"
//"arXiv"
//"doi"
//"grid"
//"info:eu-repo/dai"
//"orcidworkid"
//"pmc"
//"pmid"
//"urn"
//"who"
//"drks"
//"pdb"