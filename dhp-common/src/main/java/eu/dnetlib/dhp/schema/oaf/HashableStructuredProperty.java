/*
 * Copyright (c) 2024.
 * SPDX-FileCopyrightText: © 2023 Consiglio Nazionale delle Ricerche
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package eu.dnetlib.dhp.schema.oaf;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class HashableStructuredProperty extends StructuredProperty {

	private static final long serialVersionUID = 8371670185221126045L;

	public static HashableStructuredProperty newInstance(String value, Qualifier qualifier, DataInfo dataInfo) {
		if (value == null) {
			return null;
		}
		final HashableStructuredProperty sp = new HashableStructuredProperty();
		sp.setValue(value);
		sp.setQualifier(qualifier);
		sp.setDataInfo(dataInfo);
		return sp;
	}

	public static HashableStructuredProperty newInstance(StructuredProperty sp) {
		HashableStructuredProperty hsp = new HashableStructuredProperty();
		hsp.setQualifier(sp.getQualifier());
		hsp.setValue(sp.getValue());
		hsp.setQualifier(sp.getQualifier());
		return hsp;
	}

	public static StructuredProperty toStructuredProperty(HashableStructuredProperty hsp) {
		StructuredProperty sp = new StructuredProperty();
		sp.setQualifier(hsp.getQualifier());
		sp.setValue(hsp.getValue());
		sp.setQualifier(hsp.getQualifier());
		return sp;
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder(11, 91)
			.append(getQualifier().getClassid())
			.append(getQualifier().getSchemeid())
			.append(getValue())
			.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (obj == this) {
			return true;
		}
		if (obj.getClass() != getClass()) {
			return false;
		}
		final HashableStructuredProperty rhs = (HashableStructuredProperty) obj;
		return new EqualsBuilder()
			.append(getQualifier().getClassid(), rhs.getQualifier().getClassid())
			.append(getQualifier().getSchemeid(), rhs.getQualifier().getSchemeid())
			.append(getValue(), rhs.getValue())
			.isEquals();
	}
}
