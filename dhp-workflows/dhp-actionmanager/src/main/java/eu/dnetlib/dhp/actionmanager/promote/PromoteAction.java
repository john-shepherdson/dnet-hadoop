/*
 * Copyright (c) 2024.
 * SPDX-FileCopyrightText: © 2023 Consiglio Nazionale delle Ricerche
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package eu.dnetlib.dhp.actionmanager.promote;

/** Encodes the Actionset promotion strategies */
public class PromoteAction {

	/** The supported actionset promotion strategies
	 *
	 * ENRICH: promotes only records in the actionset matching another record in the
	 *  graph and enriches them applying the given MergeAndGet strategy
	 * UPSERT: promotes all the records in an actionset, matching records are updated
	 *  using the given MergeAndGet strategy, the non-matching record as inserted as they are.
	 */
	public enum Strategy {
		ENRICH, UPSERT
	}

	/**
	 * Returns the string representation of the join type implementing the given PromoteAction.
	 *
	 * @param strategy the strategy to be used to promote the Actionset contents
	 * @return the join type used to implement the promotion strategy
	 */
	public static String joinTypeForStrategy(PromoteAction.Strategy strategy) {
		switch (strategy) {
			case ENRICH:
				return "left_outer";
			case UPSERT:
				return "full_outer";
			default:
				throw new IllegalStateException("unsupported PromoteAction: " + strategy.toString());
		}
	}
}
