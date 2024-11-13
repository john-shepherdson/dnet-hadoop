package eu.dnetlib.dhp.sx.bio.pubmed;

/**
 * The type Pubmed Affiliation.
 *
 * @author Sandro La Bruzzo
 */
public class PMAffiliation {

    private  String name;

    private PMIdentifier identifier;

    public PMAffiliation() {

    }
    public PMAffiliation(String name, PMIdentifier identifier) {
        this.name = name;
        this.identifier = identifier;
    }

    public String getName() {
        return name;
    }

    public PMAffiliation setName(String name) {
        this.name = name;
        return this;
    }

    public PMIdentifier getIdentifier() {
        return identifier;
    }

    public PMAffiliation setIdentifier(PMIdentifier identifier) {
        this.identifier = identifier;
        return this;
    }
}
