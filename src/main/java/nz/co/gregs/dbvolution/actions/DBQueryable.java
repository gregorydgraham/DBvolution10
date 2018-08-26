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
package nz.co.gregs.dbvolution.actions;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.List;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.exceptions.AccidentalCartesianJoinException;

/**
 *
 * @author greg
 */
public interface DBQueryable {

	/**
	 * Performs the DB query and returns a list of all actions performed in the
	 * process.
	 *
	 * <p>
	 * The supplied row will be changed by the action in an appropriate way,
	 * however the Action will contain an unchanged and unchangeable copy of the
	 * row for internal use.
	 *
	 * @param db the target database.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return The complete list of all actions performed to complete this action
	 * on the database
	 * @throws SQLException Database operations may throw SQLExceptions
	 */
	public DBQueryable query(DBDatabase db) throws SQLException, AccidentalCartesianJoinException, AccidentalBlankQueryException;

	public List<DBQueryRow> getAllRows() throws SQLException, SQLTimeoutException, AccidentalBlankQueryException, AccidentalCartesianJoinException ;

	public String toSQLString(DBDatabase db);

}
