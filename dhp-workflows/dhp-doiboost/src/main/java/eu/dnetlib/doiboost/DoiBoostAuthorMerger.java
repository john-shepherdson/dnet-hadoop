
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
