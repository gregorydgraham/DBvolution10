/*
 * Copyright 2017 greg.
 *
 * This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. 
 * To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/4.0/ 
 * or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.
 * 
 * You are free to:
 *     Share - copy and redistribute the material in any medium or format
 *     Adapt - remix, transform, and build upon the material
 * 
 *     The licensor cannot revoke these freedoms as long as you follow the license terms.               
 *     Under the following terms:
 *                 
 *         Attribution - 
 *             You must give appropriate credit, provide a link to the license, and indicate if changes were made. 
 *             You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.
 *         NonCommercial - 
 *             You may not use the material for commercial purposes.
 *         ShareAlike - 
 *             If you remix, transform, or build upon the material, 
 *             you must distribute your contributions under the same license as the original.
 *         No additional restrictions - 
 *             You may not apply legal terms or technological measures that legally restrict others from doing anything the 
 *             license permits.
 * 
 * Check the Creative Commons website for any details, legalese, and updates.
 */
package nz.co.gregs.dbvolution.internal.query;

import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;

/**
 * Helper class to store the progress of turning the DBQuery into an actual
 * piece of SQL.
 *
 */
public class QueryState {

	//		private QueryGraph graph;
	final List<BooleanExpression> remainingExpressions;
	private final List<BooleanExpression> consumedExpressions = new ArrayList<>();
	private final List<String> requiredConditions = new ArrayList<>();
	private final List<String> optionalConditions = new ArrayList<>();
	private boolean queryIsFullOuterJoin = true;
	private boolean queryIsLeftOuterJoin = true;
	private boolean hasBeenOrdered = false;
	private final List<DBRow> joinedTables = new ArrayList<>();
	private final List<DBExpression> joinedComplexExpressions = new ArrayList<>();

	public QueryState(QueryDetails details) {
		this.remainingExpressions = new ArrayList<>(details.getConditions());
	}

	public Iterable<BooleanExpression> getRemainingExpressions() {
		return new ArrayList<>(remainingExpressions);
	}

	public void consumeExpression(BooleanExpression expr) {
		remainingExpressions.remove(expr);
		consumedExpressions.add(expr);
	}

	/**
	 * Adds a condition that pertains to a required table.
	 *
	 * @param conditionClause	conditionClause
	 */
	public void addRequiredCondition(String conditionClause) {
		requiredConditions.add(conditionClause);
	}

	public void addRequiredConditions(List<String> conditionClauses) {
		requiredConditions.addAll(conditionClauses);
	}

	/**
	 * Returns all the current conditions that pertain to required tables.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a list of SQL snippets representing required conditions.
	 */
	public List<String> getRequiredConditions() {
		return requiredConditions;
	}

	/**
	 * Add conditions that pertain to optional tables.
	 *
	 * @param conditionClauses	conditionClauses
	 */
	public void addOptionalConditions(List<String> conditionClauses) {
		optionalConditions.addAll(conditionClauses);
	}

	/**
	 * Returns all the current conditions that pertain to options tables.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a list of SQL snippets representing conditions on optional tables.
	 */
	public List<String> getOptionalConditions() {
		return optionalConditions;
	}

	public void addedFullOuterJoinToQuery() {
		queryIsFullOuterJoin = queryIsFullOuterJoin && true;
		queryIsLeftOuterJoin = false;
	}

	public void addedLeftOuterJoinToQuery() {
		queryIsLeftOuterJoin = queryIsLeftOuterJoin && true;
		queryIsFullOuterJoin = false;
	}

	public void addedInnerJoinToQuery() {
		queryIsLeftOuterJoin = false;
		queryIsFullOuterJoin = false;
	}

	public boolean isFullOuterJoin() {
		return queryIsFullOuterJoin;
	}

	public void addAllToRemainingExpressions(List<BooleanExpression> relationshipsAsBooleanExpressions) {
		remainingExpressions.addAll(relationshipsAsBooleanExpressions);
	}

	public synchronized boolean hasBeenOrdered() {
		return hasBeenOrdered;
	}

	public synchronized void setHasBeenOrdered(boolean b) {
		hasBeenOrdered = b;
	}

	boolean hasNotHadATableAddedYet() {
		return joinedTables.isEmpty() && joinedComplexExpressions.isEmpty();
	}

	public List<DBRow> getJoinedTables() {
		return joinedTables;
	}

	public void addJoinedTable(DBRow tabRow) {
		joinedTables.add(tabRow);
	}

	void addJoinedExpression(DBExpression expression) {
		joinedComplexExpressions.add(expression);
	}

	boolean hasHadATableAdded() {
		return !getJoinedTables().isEmpty()
				|| !getJoinedComplexExpressions().isEmpty();
	}

	List<DBExpression> getJoinedComplexExpressions() {
		return joinedComplexExpressions;
	}

}
