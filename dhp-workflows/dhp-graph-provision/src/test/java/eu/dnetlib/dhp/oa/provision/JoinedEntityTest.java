/*
 * Copyright (c) 2024.
 * SPDX-FileCopyrightText: © 2023 Consiglio Nazionale delle Ricerche
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package eu.dnetlib.dhp.oa.provision;

import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.oa.provision.model.JoinedEntity;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.Journal;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.Publication;

class JoinedEntityTest {

	private static final Logger log = LoggerFactory.getLogger(JoinedEntityTest.class);

	@Test
	void test_serialisation() throws IOException {

		Publication p = new Publication();
		p.setId("p1");
		Journal j = new Journal();
		j.setIss("1234-5678");
		p.setJournal(j);

		Organization o = new Organization();
		o.setId("o1");
		Field<String> lName = new Field<>();
		lName.setValue("CNR");
		o.setLegalname(lName);

		ObjectMapper mapper = new ObjectMapper();
		mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

		final String json = mapper.writeValueAsString(new JoinedEntity(p));
		log.info(json);

	}

}
