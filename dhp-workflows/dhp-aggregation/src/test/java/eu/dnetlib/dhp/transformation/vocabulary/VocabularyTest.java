package eu.dnetlib.dhp.transformation.vocabulary;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
public class VocabularyTest {



    @Test
    public void testLoadVocabulary() throws Exception {

        final Vocabulary vocabulary = VocabularyHelper.getVocabularyFromAPI("dnet:languages");
        assertEquals("dnet:languages",vocabulary.getName());


    }
}
