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

	CREATE_SEQUENCE(true,false,false),
	CREATE_TABLE(true, false, false),
	CREATE_TRIGGER(true, false, false),
	DELETE_ROW,
	DELETE_BY_EXAMPLE,
	DROP_SEQUENCE(true, false, false),
	DROP_TRIGGER(true, false, false),
	DROP_TABLE(true, true, false),
	SIMPLE_SELECT_QUERY,
	RECURSIVE_QUERY,
	CREATE_DATABASE(true, false, false),
	CREATE_USER(true, false, false),
	CREATE_FOREIGN_KEY(true, false, false),
	DROP_FOREIGN_KEY(true, false, false),
	CREATE_INDEX(true, false, false),
	CHECK_TABLE_EXISTS,
	ADD_COLUMN_TO_TABLE(true, false, false),
	BULK_INSERT,
	BULK_DELETE,
	INSERT_ROW,
	INSERT_ROW_WITH_LARGE_OBJECT,
	RETRIEVE_LAST_INSERT,
	UPDATE_SEQUENCE,
	UPDATE_ROW,
	CREATE_DOMAIN(true, false, false),
	DROP_FUNCTION(true, false, false),
	CREATE_FUNCTION(true, false, false),
	ALLOW_IDENTITY_INSERT,
	CREATE_EXTENSION(true, false, false),
	SET_TIMEZONE,
	CREATE_TRIGGER_BASED_IDENTITY(true, false, false), 
	DROP_TRIGGER_BASED_IDENTITY(true, false, false), 
	CHECK_TABLE_STRUCTURE,
	UPDATE_ROW_WITH_LARGE_OBJECT,
	MIGRATION,
	INSERT_QUERY;
	private boolean isDDL;

	public boolean isDDL() {
		return isDDL;
	}

	public boolean isDropTable() {
		return isDropTable;
	}

	public boolean isDropDatabase() {
		return isDropDatabase;
	}
	private boolean isDropTable;
	private boolean isDropDatabase;
	
	private QueryIntention(){
		this(false, false,false);
	}
	private QueryIntention(boolean isDDL, boolean isDropTable, boolean isDropDatabase){
		this.isDDL = isDDL;
		this.isDropTable = isDropTable;
		this.isDropDatabase = isDropDatabase;
		
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
}
