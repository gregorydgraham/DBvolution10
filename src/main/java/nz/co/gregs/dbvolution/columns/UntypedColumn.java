/*
 * Copyright 2018 gregorygraham.
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
package nz.co.gregs.dbvolution.columns;

import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBUntypedValue;
import nz.co.gregs.dbvolution.expressions.SortProvider;
import nz.co.gregs.dbvolution.expressions.UntypedExpression;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 *
 * @author gregorygraham
 */
public class UntypedColumn extends UntypedExpression implements ColumnProvider {

	private static final long serialVersionUID = 1l;

	private AbstractColumn column;

	private UntypedColumn() {

	}

	/**
	 * Create a StringColumn for the supplied field of the supplied row
	 *
	 * @param row the row containing the field
	 * @param field the field defining the column
	 */
	public UntypedColumn(RowDefinition row, Object field) {
		this.column = new AbstractColumn(row, field);
	}

	/**
	 * Create a StringColumn for the supplied field of the supplied row
	 *
	 * @param row the row containing the field
	 * @param field the field defining the column
	 */
	public UntypedColumn(RowDefinition row, DBUntypedValue field) {
		this.column = new AbstractColumn(row, field);
	}

	@Override
	public synchronized UntypedColumn copy() {
		final AbstractColumn col = getColumn();
		final DBRow row = col.getInstanceOfRow();
		UntypedColumn newInstance = new UntypedColumn(row, col.getAppropriateQDTFromRow(row));
		return newInstance;
	}

	@Override
	public AbstractColumn getColumn() {
		return column;
	}

	@Override
	public void setUseTableAlias(boolean useTableAlias) {
		this.column.setUseTableAlias(useTableAlias);
	}

	@Override
	public String toSQLString(DBDefinition db) {
		return column.toSQLString(db);
	}

	@Override
	public SortProvider.Column getSortProvider() {
		return column.getSortProvider();
	}

}
