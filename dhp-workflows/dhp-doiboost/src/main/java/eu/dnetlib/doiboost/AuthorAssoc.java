
package eu.dnetlib.doiboost;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import eu.dnetlib.dhp.schema.oaf.Author;

/**
 * This class stores the association information between the enriching author and the possibly enriched ones.
 * It also contains the value of the similarity score between the enriching author and the possibly enriched ones.
 * Possibly enriched authors with the same similarity score with the enriching are put in the to_be_enriched list.
 */
public class AuthorAssoc implements Serializable {
	private Double score;
	private List<Author> to_be_enriched;
	private Author with_enricheing_content;

	public Double getScore() {
		return score;
	}

	public void setScore(Double score) {
		this.score = score;
	}

	public List<Author> getTo_be_enriched() {
		return to_be_enriched;
	}

	public void setTo_be_enriched(List<Author> to_be_enriched) {
		this.to_be_enriched = to_be_enriched;
	}

	public Author getWith_enricheing_content() {
		return with_enricheing_content;
	}

	public void setWith_enricheing_content(Author with_enricheing_content) {
		this.with_enricheing_content = with_enricheing_content;
	}

	public static AuthorAssoc newInstance(Author a) {
		AuthorAssoc ret = new AuthorAssoc();
		ret.score = 0.0;
		ret.to_be_enriched = new ArrayList<>();
		ret.with_enricheing_content = a;

		return ret;

	}
}
