/*
 * Copyright 2021 Gregory Graham.
 *
 * Commercial licenses are available, please contact info@gregs.co.nz for details.
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

import java.sql.SQLException;
import java.sql.Statement;
import nz.co.gregs.dbvolution.databases.QueryIntention;
import nz.co.gregs.dbvolution.utility.StringCheck;

/**
 *
 * @author gregorygraham
 */
public class StatementDetails {

	private final String sql;
	private Exception exception;
	private QueryIntention intention;

	private String label = "Unlabelled SQL";
	private boolean ignoreExceptions = false;
	private boolean withGeneratedKeys = false;
	private String namedPKColumn;

	public StatementDetails(String label, QueryIntention intent, String sql) {
		this(label, intent, sql, null, false, false, "");
	}

	public StatementDetails copy() {
		return new StatementDetails(label, intention, sql, exception, withGeneratedKeys, ignoreExceptions, namedPKColumn);
	}

	public StatementDetails(String label, QueryIntention intent, String sql, Exception except, boolean generatedKeys, boolean ignoreExceptions, String pkColumn) {
		this.label = label;
		this.sql = sql;
		this.intention = intent;
		this.exception = except;
		this.withGeneratedKeys = generatedKeys;
		this.ignoreExceptions = ignoreExceptions;
		this.namedPKColumn = pkColumn;
	}

	public String getSql() {
		return sql;
	}

	public Exception getException() {
		return exception;
	}

	public QueryIntention getIntention() {
		return intention;
	}

	public String getLabel() {
		return label;
	}

	public StatementDetails withLabel(String label) {
		this.label = label;
		return this;
	}

	public boolean isIgnoreExceptions() {
		return ignoreExceptions;
	}

	public final void setIgnoreExceptions(boolean ignoreExceptions) {
		this.ignoreExceptions = ignoreExceptions;
	}

	public boolean requiresGeneratedKeys() {
		return withGeneratedKeys;
	}

	public StatementDetails withGeneratedKeys() {
		this.withGeneratedKeys = true;
		return this;
	}

	/**
	 * Calls the appropriate execute method on the Statement and returns the boolean result.
	 * 
	 * @param stmt the statement on which to excute this SQL command
	 * @return the result of calling the appropriate execute method
	 * @throws SQLException database errors are propogated
	 */
	public boolean execute(Statement stmt) throws SQLException {
		boolean execute;
		if (StringCheck.isNotEmptyNorNull(namedPKColumn)) {
			execute = stmt.execute(sql, new String[]{namedPKColumn});
		} else if (requiresGeneratedKeys()) {
			execute = stmt.execute(sql, Statement.RETURN_GENERATED_KEYS);
		} else {
			execute = stmt.execute(sql);
		}
		return execute;
	}

	public StatementDetails withException(SQLException exp2) {
		this.exception = exp2;
		return this;
	}

	public StatementDetails withNamedPKColumn(String primaryKeyForRetrievingGeneratedKeys) {
		namedPKColumn = primaryKeyForRetrievingGeneratedKeys;
		return this;
	}

	public StatementDetails withIntention(QueryIntention queryIntention) {
		this.intention = queryIntention;
		return this;
	}

}
