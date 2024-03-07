
package eu.dnetlib.dhp.sx.bio.pubmed;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * This class represent an instance of Pubmed Article extracted from the native XML
 *
 * @author Sandro La Bruzzo
 */

public class PMArticle implements Serializable {

	/**
	 * the Pubmed Identifier
	 */
	private String pmid;

	private String pmcId;

	/**
	 * the DOI
	 */
	private String doi;
	/**
	 * the Pubmed Date extracted from <PubmedPubDate> Specifies a date significant to either the article's history or the citation's processing.
	 * All <History> dates will have a <Year>, <Month>, and <Day> elements. Some may have an <Hour>, <Minute>, and <Second> element(s).
	 */
	private String date;
	/**
	 * This is an 'envelop' element that contains various elements describing the journal cited; i.e., ISSN, Volume, Issue, and PubDate and author name(s), however, it does not contain data itself.
	 */
	private PMJournal journal;
	/**
	 * The full journal title (taken from NLM cataloging data following NLM rules for how to compile a serial name) is exported in this element. Some characters that are not part of the NLM MEDLINE/PubMed Character Set reside in a relatively small number of full journal titles. The NLM journal title abbreviation is exported in the <MedlineTA> element.
	 */
	private String title;
	/**
	 * English-language abstracts are taken directly from the published article.
	 * If the article does not have a published abstract, the National Library of Medicine does not create one,
	 * thus the record lacks the <Abstract> and <AbstractText> elements. However, in the absence of a formally
	 * labeled abstract in the published article, text from a substantive "summary", "summary and conclusions" or "conclusions and summary" may be used.
	 */
	private String description;
	/**
	 * the language in which an article was published is recorded in <Language>.
	 * All entries are three letter abbreviations stored in lower case, such as eng, fre, ger, jpn, etc. When a single
	 * record contains more than one language value the XML export program extracts the languages in alphabetic order by the 3-letter language value.
	 *  Some records provided by collaborating data producers may contain the value und to identify articles whose language is undetermined.
	 */
	private String language;

	/**
	 * NLM controlled vocabulary, Medical Subject Headings (MeSH®), is used to characterize the content of the articles represented by MEDLINE citations.	 *
	 */
	private final List<PMSubject> subjects = new ArrayList<>();
	/**
	 * This element is used to identify the type of article indexed for MEDLINE;
	 * it characterizes the nature of the information or the manner in which it is conveyed as well as the type of
	 * research support received (e.g., Review, Letter, Retracted Publication, Clinical Conference, Research Support, N.I.H., Extramural).
	 */
	private final List<PMSubject> publicationTypes = new ArrayList<>();
	/**
	 * Personal and collective (corporate) author names published with the article are found in <AuthorList>.
	 */
	private List<PMAuthor> authors = new ArrayList<>();

	/**
	 * <GrantID> contains the research grant or contract number (or both) that designates financial support by any agency of the United States Public Health Service
	 * or any institute of the National Institutes of Health. Additionally, beginning in late 2005, grant numbers are included for many other US and non-US funding agencies and organizations.
	 */
	private final List<PMGrant> grants = new ArrayList<>();

	/**
	 * get the DOI
	 * @return a DOI
	 */
	public String getDoi() {
		return doi;
	}

	/**
	 * Set the DOI
	 * @param doi a DOI
	 */
	public void setDoi(String doi) {
		this.doi = doi;
	}

	/**
	 * get the Pubmed Identifier
	 * @return the PMID
	 */
	public String getPmid() {
		return pmid;
	}

	/**
	 * set the Pubmed Identifier
	 * @param pmid the Pubmed Identifier
	 */
	public void setPmid(String pmid) {
		this.pmid = pmid;
	}

	/**
	 * the Pubmed Date extracted from <PubmedPubDate> Specifies a date significant to either the article's history or the citation's processing.
	 * All <History> dates will have a <Year>, <Month>, and <Day> elements. Some may have an <Hour>, <Minute>, and <Second> element(s).
	 *
	 * @return the Pubmed Date
	 */
	public String getDate() {
		return date;
	}

	/**
	 * Set the pubmed Date
	 * @param date
	 */
	public void setDate(String date) {
		this.date = date;
	}

	/**
	 * The full journal title (taken from NLM cataloging data following NLM rules for how to compile a serial name) is exported in this element.
	 * Some characters that are not part of the NLM MEDLINE/PubMed Character Set reside in a relatively small number of full journal titles.
	 * The NLM journal title abbreviation is exported in the <MedlineTA> element.
	 *
	 * @return the pubmed Journal Extracted
	 */
	public PMJournal getJournal() {
		return journal;
	}

	/**
	 * Set the mapped pubmed Journal
	 * @param journal
	 */
	public void setJournal(PMJournal journal) {
		this.journal = journal;
	}

	/**
	 * <ArticleTitle> contains the entire title of the journal article. <ArticleTitle> is always in English;
	 * those titles originally published in a non-English language and translated for <ArticleTitle> are enclosed in square brackets.
	 * All titles end with a period unless another punctuation mark such as a question mark or bracket is present.
	 * Explanatory information about the title itself is enclosed in parentheses, e.g.: (author's transl).
	 * Corporate/collective authors may appear at the end of <ArticleTitle> for citations up to about the year 2000.
	 *
	 *  @return the extracted pubmed Title
	 */
	public String getTitle() {
		return title;
	}

	/**
	 * set the pubmed title
	 * @param title
	 */
	public void setTitle(String title) {
		this.title = title;
	}

	/**
	 * English-language abstracts are taken directly from the published article.
	 * If the article does not have a published abstract, the National Library of Medicine does not create one,
	 * thus the record lacks the <Abstract> and <AbstractText> elements. However, in the absence of a formally
	 * labeled abstract in the published article, text from a substantive "summary", "summary and conclusions" or "conclusions and summary" may be used.
	 *
	 * @return the Mapped Pubmed Article Abstracts
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * Set the Mapped Pubmed Article Abstracts
	 * @param description
	 */
	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * Personal and collective (corporate) author names published with the article are found in <AuthorList>.
	 *
	 * @return get the Mapped Authors lists
	 */
	public List<PMAuthor> getAuthors() {
		return authors;
	}

	/**
	 * Set the Mapped Authors lists
	 * @param authors
	 */
	public void setAuthors(List<PMAuthor> authors) {
		this.authors = authors;
	}

	/**
	 * This element is used to identify the type of article indexed for MEDLINE;
	 * it characterizes the nature of the information or the manner in which it is conveyed as well as the type of
	 * research support received (e.g., Review, Letter, Retracted Publication, Clinical Conference, Research Support, N.I.H., Extramural).
	 *
	 * @return the mapped Subjects
	 */
	public List<PMSubject> getSubjects() {
		return subjects;
	}

	/**
	 *
	 * the language in which an article was published is recorded in <Language>.
	 * All entries are three letter abbreviations stored in lower case, such as eng, fre, ger, jpn, etc. When a single
	 * record contains more than one language value the XML export program extracts the languages in alphabetic order by the 3-letter language value.
	 *  Some records provided by collaborating data producers may contain the value und to identify articles whose language is undetermined.
	 *
	 * @return The mapped Language
	 */
	public String getLanguage() {
		return language;
	}

	/**
	 *
	 *  Set The mapped Language
	 *
	 * @param language the mapped Language
	 */
	public void setLanguage(String language) {
		this.language = language;
	}

	/**
	 *  This element is used to identify the type of article indexed for MEDLINE;
	 * it characterizes the nature of the information or the manner in which it is conveyed as well as the type of
	 * research support received (e.g., Review, Letter, Retracted Publication, Clinical Conference, Research Support, N.I.H., Extramural).
	 *
	 * @return the mapped Publication Type
	 */
	public List<PMSubject> getPublicationTypes() {
		return publicationTypes;
	}

	/**
	 * <GrantID> contains the research grant or contract number (or both) that designates financial support by any agency of the United States Public Health Service
	 * or any institute of the National Institutes of Health. Additionally, beginning in late 2005, grant numbers are included for many other US and non-US funding agencies and organizations.
	 * @return the mapped grants
	 */

	public List<PMGrant> getGrants() {
		return grants;
	}

	public String getPmcId() {
		return pmcId;
	}

	public PMArticle setPmcId(String pmcId) {
		this.pmcId = pmcId;
		return this;
	}
}
