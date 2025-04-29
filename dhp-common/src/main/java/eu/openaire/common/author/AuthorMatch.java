
package eu.openaire.common.author;

import static org.apache.commons.lang3.builder.ToStringStyle.NO_CLASS_NAME_STYLE;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * A class representing the successful match between a base author and an enriching author.
 *
 * <p>This class is designed to facilitate the comparison and potential matching of a
 * base author (UA) with one of many enriching authors (CA). It provides a structured
 * way to store and process author matching data.</p>
 *
 * @param <BA> The type representing the base author. This class contains relevant data
 *             about an author whose identity needs to be matched with potential candidates.
 * @param <EA> The type representing the enriching author. This class contains relevant data
 *             about author who is potential match for the base author.
 */
public class AuthorMatch<BA, EA> {
	private BA baseAuthor; // The base author
	private EA enrichingAuthor; // The enriching author
	private String stepName; // The step name associated with the match
	private double confidence; // Confidence score of the match

	/**
	 * Constructs an AuthorMatch object with specified base author, enriching author, step name, and confidence score.
	 *
	 * @param baseAuthor      The base author
	 * @param enrichingAuthor The enriching author
	 * @param stepName        The step in which this match occurred
	 * @param confidence      The confidence score of the match
	 */
	public AuthorMatch(BA baseAuthor, EA enrichingAuthor, String stepName, double confidence) {
		this.baseAuthor = baseAuthor;
		this.enrichingAuthor = enrichingAuthor;
		this.stepName = stepName;
		this.confidence = confidence;
	}

	/**
	 * Creates a new AuthorMatch instance with a modified step name.
	 *
	 * @param stepName The new step name
	 * @return A new AuthorMatch instance with the updated step name
	 */
	public AuthorMatch<BA, EA> withStepName(String stepName) {
		return new AuthorMatch<>(this.baseAuthor, this.enrichingAuthor, stepName, this.confidence);
	}

	static public <UA, CA> AuthorMatch<UA, CA> of(UA baseAuthor, CA enrichingAuthor, double confidence) {
		return new AuthorMatch<>(baseAuthor, enrichingAuthor, "", confidence);
	}

	/**
	 * Gets the base author.
	 *
	 * @return The base author
	 */
	public BA getBaseAuthor() {
		return baseAuthor;
	}

	public void setBaseAuthor(BA baseAuthor) {
		this.baseAuthor = baseAuthor;
	}

	/**
	 * Gets the enriching author.
	 *
	 * @return The enriching author
	 */
	public EA getEnrichingAuthor() {
		return enrichingAuthor;
	}

	public void setEnrichingAuthor(EA enrichingAuthor) {
		this.enrichingAuthor = enrichingAuthor;
	}

	/**
	 * Gets the step name of the match.
	 *
	 * @return The step name
	 */
	public String getStepName() {
		return stepName;
	}

	public void setStepName(String stepName) {
		this.stepName = stepName;
	}

	/**
	 * Gets the confidence score of the match.
	 *
	 * @return The confidence score
	 */
	public double getConfidence() {
		return confidence;
	}

	public void setConfidence(double confidence) {
		this.confidence = confidence;
	}

	/**
	 * Returns a string representation of the AuthorMatch object.
	 *
	 * @return A string describing the object
	 */
	@Override
	public String toString() {
		return new ToStringBuilder(this, NO_CLASS_NAME_STYLE)
			.append("baseAuthor", baseAuthor)
			.append("enrichingAuthor", enrichingAuthor)
			.append("confidence", confidence)
			.append("stepName", stepName)
			.toString();
	}
}
