/*
 * Copyright (c) 2025.
 * SPDX-FileCopyrightText: © 2023 Consiglio Nazionale delle Ricerche
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package eu.dnetlib.dhp.oa.provision.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.schema.solr.Funding;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;

import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
public class ProvisionModelSupportTest {

    @Mock
    private ISLookUpService isLookUpService;

    private VocabularyGroup vocabularies;

    @BeforeEach
    void beforeEach() throws IOException, ISLookUpException {
        lenient().when(isLookUpService.quickSearchProfile(VocabularyGroup.VOCABULARIES_XQUERY)).thenReturn(vocs());
        lenient()
                .when(isLookUpService.quickSearchProfile(VocabularyGroup.VOCABULARY_SYNONYMS_XQUERY))
                .thenReturn(synonyms());

        vocabularies = VocabularyGroup.loadVocsFromIS(isLookUpService);
    }

    @Test
    void testMapFunding() throws JsonProcessingException {

        List<String> fTrees = Lists.newArrayList("<fundingtree><funder><id>fct_________::FCT</id><shortname>FCT</shortname><name>Fundação para a Ciência e a Tecnologia, I.P.</name><jurisdiction>PT</jurisdiction></funder><funding_level_0><id>fct_________::FCT::Projetos de IC&amp;DT Portugal Índia</id><description>Projetos de IC&amp;DT Portugal Índia</description><name>Projetos de IC&amp;DT Portugal Índia</name><parent/><class>fct:program</class></funding_level_0></fundingtree>");

        final Funding funding = ProvisionModelSupport.mapFunding(fTrees, vocabularies);

        Assertions.assertNotNull(funding);

        ObjectMapper objectMapper = new ObjectMapper();

        System.out.println(objectMapper.writeValueAsString(funding));

        Assertions.assertEquals("fct_________::FCT", funding.getFunder().getId());
        Assertions.assertEquals("fct_________::FCT::Projetos de IC&DT Portugal Índia", funding.getLevel0().getId());
    }

    private List<String> vocs() throws IOException {
        return IOUtils
                .readLines(
                        Objects
                                .requireNonNull(
                                        getClass().getResourceAsStream("/eu/dnetlib/dhp/oa/provision/model/terms.txt")),
                        Charset.defaultCharset());
    }

    private List<String> synonyms() throws IOException {
        return IOUtils
                .readLines(
                        Objects
                                .requireNonNull(
                                        getClass().getResourceAsStream("/eu/dnetlib/dhp/oa/provision/model/synonyms.txt")),
                        Charset.defaultCharset());
    }
}
