
package eu.openaire.common.author;

import static org.apache.commons.lang3.builder.ToStringStyle.NO_CLASS_NAME_STYLE;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * A class representing the successful match between an unmatched author and a candidate author.
 *
 * <p>This class is designed to facilitate the comparison and potential matching of an
 * unmatched author (UA) with one of many candidate authors (CA). It provides a structured
 * way to store and process author matching data.</p>
 *
 * @param <UA> The type representing the unmatched author. This class contains relevant data
 *             about an author whose identity needs to be matched with potential candidates.
 * @param <CA> The type representing the candidate author. This class contains relevant data
 *             about author who is potential match for the unmatched author.
 */
public class AuthorMatch<UA, CA> {
	private UA matchedAuthor; // The matched author
	private CA matchedCandidate; // The matched candidate
	private String stepName; // The step name associated with the match
	private double confidence; // Confidence score of the match

	/**
	 * Constructs an AuthorMatch object with specified author, candidate, step name, and confidence score.
	 *
	 * @param matchedAuthor    The matched author
	 * @param matchedCandidate The matched candidate
	 * @param stepName         The step in which this match occurred
	 * @param confidence       The confidence score of the match
	 */
	public AuthorMatch(UA matchedAuthor, CA matchedCandidate, String stepName, double confidence) {
		this.matchedAuthor = matchedAuthor;
		this.matchedCandidate = matchedCandidate;
		this.stepName = stepName;
		this.confidence = confidence;
	}

	/**
	 * Creates a new AuthorMatch instance with a modified step name.
	 *
	 * @param stepName The new step name
	 * @return A new AuthorMatch instance with the updated step name
	 */
	public AuthorMatch<UA, CA> withStepName(String stepName) {
		return new AuthorMatch<>(this.matchedAuthor, this.matchedCandidate, stepName, this.confidence);
	}

	static public <UA, CA> AuthorMatch<UA, CA> of(UA matchedAuthor, CA matchedCandidate, double confidence) {
		return new AuthorMatch<>(matchedAuthor, matchedCandidate, "", confidence);
	}

	/**
	 * Gets the matched author.
	 *
	 * @return The matched author
	 */
	public UA getMatchedAuthor() {
		return matchedAuthor;
	}

	public void setMatchedAuthor(UA matchedAuthor) {
		this.matchedAuthor = matchedAuthor;
	}

	/**
	 * Gets the matched candidate.
	 *
	 * @return The matched candidate
	 */
	public CA getMatchedCandidate() {
		return matchedCandidate;
	}

	public void setMatchedCandidate(CA matchedCandidate) {
		this.matchedCandidate = matchedCandidate;
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
			.append("matchedAuthor", matchedAuthor)
			.append("matchedCandidate", matchedCandidate)
			.append("confidence", confidence)
			.append("stepName", stepName)
			.toString();
	}
}
