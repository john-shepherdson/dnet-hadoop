package eu.dnetlib.dhp.common.vocabulary;

import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.mockito.Mockito.lenient;


@ExtendWith(MockitoExtension.class)
public class VocabularyTest {


    @Mock
    protected ISLookUpService isLookUpService;

    protected VocabularyGroup vocabularies;

    @BeforeEach
    public void setUpVocabulary() throws ISLookUpException, IOException {

        lenient().when(isLookUpService.quickSearchProfile(VocabularyGroup.VOCABULARIES_XQUERY)).thenReturn(vocs());

        lenient()
                .when(isLookUpService.quickSearchProfile(VocabularyGroup.VOCABULARY_SYNONYMS_XQUERY))
                .thenReturn(synonyms());
        vocabularies = VocabularyGroup.loadVocsFromIS(isLookUpService);
    }

    private static List<String> vocs() throws IOException {
        return IOUtils
                .readLines(
                        Objects
                                .requireNonNull(
                                        VocabularyTest.class.getResourceAsStream("/eu/dnetlib/dhp/transform/terms.txt")));
    }

    private static List<String> synonyms() throws IOException {
        return IOUtils
                .readLines(
                        Objects
                                .requireNonNull(
                                        VocabularyTest.class.getResourceAsStream("/eu/dnetlib/dhp/transform/synonyms.txt")));
    }


    @Test
    void testVocabularyMatch () throws  Exception{
        final String s= IOUtils.toString(this.getClass().getResourceAsStream("terms"));

        for (String s1 : s.split("\n")) {

            final Qualifier t1 = vocabularies.getSynonymAsQualifier("dnet:publication_resource", s1);

            if (t1 == null) {
                System.err.println(s1+ " Missing");
            }
            else {
                System.out.println("syn=" + s1 + " term = " + t1.getClassid());


                System.out.println(vocabularies.getSynonymAsQualifier("dnet:result_typologies", t1.getClassid()).getClassname());
            }
        }





    }
}
