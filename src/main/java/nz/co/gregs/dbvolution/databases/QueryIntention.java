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

	CREATE_SEQUENCE,
	CREATE_TABLE,
	CREATE_TRIGGER,
	DELETE_ROW,
	DROP_SEQUENCE,
	DROP_TRIGGER,
	DROP_TABLE,
	SIMPLE_SELECT_QUERY,
	RECURSIVE_QUERY,
	CREATE_DATABASE,
	CREATE_USER,
	CREATE_FOREIGN_KEY,
	DROP_FOREIGN_KEY,
	CREATE_INDEX,
	CHECK_TABLE_EXISTS,
	ADD_COLUMN_TO_TABLE,
	BULK_INSERT,
	BULK_DELETE,
	INSERT_ROW,
	RETRIEVE_LAST_INSERT,
	UPDATE_SEQUENCE,
	UPDATE_ROW,
	CREATE_DOMAIN,
	DROP_FUNCTION,
	CREATE_FUNCTION,
	ALLOW_IDENTITY_INSERT,
	CREATE_EXTENSION,
	SET_TIMEZONE,
	CREATE_TRIGGER_BASED_IDENTITY, 
	DROP_TRIGGER_BASED_IDENTITY, 
	CHECK_TABLE_STRUCTURE;

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
}
