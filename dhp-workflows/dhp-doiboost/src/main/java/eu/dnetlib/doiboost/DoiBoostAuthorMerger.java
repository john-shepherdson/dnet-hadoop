
package eu.dnetlib.doiboost;

import java.text.Normalizer;
import java.util.*;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.utils.DHPUtils;

import com.wcohen.ss.JaroWinkler;

import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

import scala.Tuple2;

/**
 * This is a version of the AuthorMerger specific for DoiBoost.
 * Here we suppose a match must exist for the authors. We compare via JaroWrinkler similarity measure each author in the list
 * that should be enriched with each author in the enriching list. For each enriching author we select the best match that is
 * the author with the highest similarity score.
 * The association is done from the enriching author to the enriched because in this way only one match per enriching author can be found
 * One enriching author can have the same maximum similarity score with more than one
 *
 *
 *
 *
 * The idea is to enrich the most similar authors having at least one
 * word of the name in
 * common
 * Quello che faccio e’ abbastanza semplice: ho una struttura dati che mantine l’informazione di associazione fra il record che puo’ possibilmente arricchire e quello che deve essere arricchito.
 * 6:22
 * Questa struttura ha la lista di autori che possono essere arricchiti, l’autore che arricchisce e lo score di similarita fra l’autore che arricchisce e gli autori arricchiti. E’ il valore di una mappa che per chiave la il fullname dell’autore che arricchisce
 * 6:23
 * per ogni autore che puo’ essere arricchito verifico se la entri nella mappa di quello che arricchisce e’ associata ad un autore con score di similarita’ piu’ basso. Se cosi’ e’ modifico l’associazione nella mappa per l’autore che arricchisce, sostituendo l’autore arricchito a cui era associato prima con quello nuovo che ha score piu’ alto. Se lo score e’ lo stesso, aggiungo il nuovo autore da arricchire alla lista degli autori associata all’autore che arricchisce
 * 6:25
 * Alla fine caso facile: ogni entry e’ associata ad un unico autore da arricchire => verifico che almeno una delle parole che sono nei due nomi sia in comune fra i due insiemi Se e’ cosi’, aggiungo i pid mancanti all’autore da arricchire dell’autore che arricchisce
 * 6:26
 * caso brutto: ci sono piu’ autori da arricchire con la stessa similarita: arricchisco quello che ha il maggior numero di parole del fullname uguali a quelle dell’autore che arricchisce. In caso di parita’ non si arricchisce
 * 6:28
 * ricordiamoci che si parte dal presupposto che un match debba esistere visto che abbiamo lo stesso doi
 * 6:29
 * di conseguenza l’autore che ha lo score di similarita’ piu’ alto fra quelli presenti ed anche una parola in comune del nome dovrebbe essere sufficiente per poterlo arricchire.
 * 6:30
 * I casi di omonimia che potrebbero portare problemi con i rank degli autori non si mappano
 */

public class DoiBoostAuthorMerger {


	public static List<Author> merge(List<List<Author>> authors,  Boolean crossref) {

		Iterator<List<Author>> it = authors.iterator();
		List<Author> author = it.next();

		while (it.hasNext()){
			List<Author> autList = it.next();
			Tuple2<List<Author>, Boolean> tmp = mergeAuthor(author, autList, crossref);
			author = tmp._1();
			crossref = tmp._2();
		}

		return author;

	}

	public static Tuple2<List<Author>, Boolean> mergeAuthor(final List<Author> baseAuthor, final List<Author> otherAuthor,
										    final Boolean crossref) {

		if(baseAuthor == null || baseAuthor.size() == 0)
			return new Tuple2<>(otherAuthor, false);
		if(otherAuthor == null || otherAuthor.size() == 0)
			return new Tuple2<>(baseAuthor, crossref);

		if(crossref) {
			enrichPidFromList(baseAuthor, otherAuthor);
			return new Tuple2<>(baseAuthor, true);
		}
		else
			if (baseAuthor.size() > otherAuthor.size()){
				enrichPidFromList(baseAuthor, otherAuthor);
				return new Tuple2<>(baseAuthor, false);
			}else{
				enrichPidFromList(otherAuthor, baseAuthor);
				return new Tuple2<>(otherAuthor, false);
			}

	}


	private static void enrichPidFromList(List<Author> base, List<Author> enrich) {
		if(base == null || enrich == null)
			return ;

		//search authors having identifiers in the enrich list
        final List<Author> authorsWithPids = enrich
                .stream()
                .filter(a -> a.getPid() != null && a.getPid().size() > 0)
                .collect(Collectors.toList());

		Map<String, AuthorAssoc> assocMap = authorsWithPids
				.stream()
				.map(
						a -> new Tuple2<>(DHPUtils.md5(a.getFullname()), AuthorAssoc.newInstance(a)))
				.collect(Collectors.toMap(Tuple2::_1, Tuple2::_2, (x1, x2) -> x1));


		//for each author in the base list, we search the best enriched match
		base.stream()
				.map(a -> new Tuple2<>(a, authorsWithPids.stream()
						.map(e -> new Tuple2<>(e, sim(a, e))).collect(Collectors.toList())))
                .forEach(t2 -> {

                    for (Tuple2<Author, Double> t : t2._2()) {
                    	String mapEntry = DHPUtils.md5(t._1().getFullname());
                    	AuthorAssoc aa = assocMap.get(mapEntry);
                    	if(aa.getScore() < t._2()){
							aa.setScore(t._2());
							aa.setTo_be_enriched(new ArrayList<>());
							aa.getTo_be_enriched().add(t2._1());
						}else if(aa.getScore() == t._2()){
                    		aa.getTo_be_enriched().add(t2._1());
						}
                    }

                });
                
		assocMap.keySet().forEach(k -> enrichAuthor(assocMap.get(k)));


	}

	private static long getCommonWords(List<String> fullEnrich, List<String> fullEnriching){
		return fullEnrich.stream().filter( w -> fullEnriching.contains(w)).count();
	}


	private static void enrichAuthor(Author enrich, Author enriching){
		//verify if some of the words in the fullname are contained in the other
		//get normalized fullname

		long commonWords = getCommonWords(normalize(enrich.getFullname()),
				normalize(enriching.getFullname()));
		if(commonWords > 0 ){
			if(enrich.getPid() == null){
				enrich.setPid(new ArrayList<>());
			}
				Set<String> aPids = enrich.getPid().stream().map(p -> pidToComparableString(p)).collect(Collectors.toSet());
			enriching.getPid().forEach(p -> {
					if (!aPids.contains(pidToComparableString(p))){
						enrich.getPid().add(p);
					}
				});
			if (enrich.getAffiliation() == null){
				if (enriching.getAffiliation() != null){
					enrich.setAffiliation(enriching.getAffiliation());
				}
			}
		}


	}

	//Verify the number of words in common. The one that has more, wins. If the number of words in common are the same we
	//enrich no author
	private static void enrichAuthor(AuthorAssoc authorAssoc) {
		if (authorAssoc.getTo_be_enriched().size() == 1){
			enrichAuthor(authorAssoc.getTo_be_enriched().get(0), authorAssoc.getWith_enricheing_content());
		}else{
			long common = 0;
			List<Author> selected = new ArrayList<>() ;
			for(Author a : authorAssoc.getTo_be_enriched()){
				long current_common = getCommonWords(normalize(a.getFullname()),
						normalize(authorAssoc.getWith_enricheing_content().getFullname()));
				if (current_common > common){
					common = current_common;
					selected = new ArrayList<>();
					selected.add(a);
				}else if(current_common == common){
					selected.add(a);
				}
			}
			if (selected.size() == 1){
				enrichAuthor(selected.get(0), authorAssoc.getWith_enricheing_content());
			}
		}

	}


	public static String pidToComparableString(StructuredProperty pid) {
		return (pid.getQualifier() != null
			? pid.getQualifier().getClassid() != null ? pid.getQualifier().getClassid().toLowerCase() : ""
			: "")
			+ (pid.getValue() != null ? pid.getValue().toLowerCase() : "");
	}




	private static Double sim(Author a, Author b) {
			return new JaroWinkler()
				.score(normalizeString(a.getFullname()), normalizeString(b.getFullname()));

	}

	private static String normalizeString(String fullname) {
		return String.join(" ", normalize(fullname));
	}


	private static List<String> normalize(final String s) {
		String[] normalized = nfd(s)
			.replaceAll("[^\\p{ASCII}]", "")
			.toLowerCase()
			// do not compact the regexes in a single expression, would cause StackOverflowError
			// in case
			// of large input strings
			.replaceAll("(\\W)+", " ")
			.replaceAll("(\\p{InCombiningDiacriticalMarks})+", " ")
			.replaceAll("(\\p{Punct})+", " ")
			.replaceAll("(\\d)+", " ")
			.replaceAll("(\\n)+", " ")
			.trim()
			.split(" ");

		Arrays.sort(normalized);

		return Arrays.asList(normalized);


	}

	private static String nfd(final String s) {
		return Normalizer.normalize(s, Normalizer.Form.NFD);
	}
}
