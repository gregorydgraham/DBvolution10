/*
 * Copyright 2019 Gregory Graham.
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
package nz.co.gregs.dbvolution.databases;

/**
 *
 * @author gregorygraham
 */
public enum QueryIntention {

	ALTER_TABLE_ADD_COLUMN(true),
	CHECK_TABLE_EXISTS(false),
	CREATE_SEQUENCE(true),
	CREATE_TABLE(true),
	CREATE_TRIGGER(true),
	DELETE_ALL_ROWS,
	DELETE_ROW,
	DELETE_BY_EXAMPLE,
	DROP_DATABASE(true),
	DROP_SEQUENCE(true),
	DROP_TRIGGER(true),
	DROP_TABLE(true),
	SIMPLE_SELECT_QUERY,
	RECURSIVE_QUERY,
	CREATE_DATABASE(true),
	CREATE_USER(true),
	CREATE_FOREIGN_KEYS(true),
	DROP_FOREIGN_KEYS(true),
	CREATE_INDEX_ON_ALL_KEYS(true),
	ADD_COLUMN_TO_TABLE(true),
	BULK_INSERT,
	BULK_DELETE,
	INSERT_ROW,
	INSERT_ROW_WITH_LARGE_OBJECT,
	RETRIEVE_LAST_INSERT,
	UPDATE_SEQUENCE,
	UPDATE_ROW,
	CREATE_DOMAIN(true),
	DROP_FUNCTION(true),
	CREATE_FUNCTION(true),
	ALLOW_IDENTITY_INSERT,
	CREATE_EXTENSION(true),
	SET_TIMEZONE,
	CREATE_TRIGGER_BASED_IDENTITY(true), 
	DROP_TRIGGER_BASED_IDENTITY(true), 
	CHECK_TABLE_STRUCTURE,
	UPDATE_ROW_WITH_LARGE_OBJECT,
	ADD_MISSING_COLUMNS_TO_TABLE(true),
	MIGRATION,
	INSERT_QUERY;
	
	private boolean isDDL;
	
	private QueryIntention(){
		this(false);
	}
	private QueryIntention(boolean isDDL){
		this.isDDL = isDDL;
		
	}

	public boolean isDDL() {
		return isDDL;
	}

	public boolean isDropTable() {
		return DROP_TABLE.equals(this);
	}

	public boolean isDropDatabase() {
		return DROP_DATABASE.equals(this);
	}

	boolean is(QueryIntention queryIntention) {
		return this.equals(queryIntention);
	}

	boolean isOneOf(QueryIntention... intents) {
		boolean result = false;
		for (QueryIntention intent : intents) {
			result = result || this.is(intent);
		}
		return result;
	}
	
	@Override
	public String toString(){
		String toString = super.toString().replace("_", " ");
		return toString;
	}

	boolean isDeleteAllRows() {
		return DELETE_ALL_ROWS.equals(this);
	}
}
