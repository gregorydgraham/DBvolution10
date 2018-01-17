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
package nz.co.gregs.dbvolution.datatypes;

import nz.co.gregs.dbvolution.columns.UntypedColumn;
import java.sql.ResultSet;
import java.sql.SQLException;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.query.RowDefinition;
import nz.co.gregs.dbvolution.results.StringResult;

/**
 * A case to represent values of indeterminate type
 *
 * @author gregorygraham
 */
public class DBUntypedValue extends QueryableDatatype<Object> implements StringResult{

	private static final long serialVersionUID = 1l;

	/**
	 * Sets the value of this DBString to the value provided.
	 *
	 * @param obj
	 */
	@Override
	public void setValue(Object obj) {
		super.setLiteralValue(obj.toString());
	}
	
	@Override
	protected void setValueFromStandardStringEncoding(String encodedValue) {
		setValue(encodedValue);
	}

	@Override
	public String getSQLDatatype() {
		return "VARCHAR(101)";
	}

	@Override
	public String formatValueForSQLStatement(DBDefinition defn) {
		if (getLiteralValue().toString().isEmpty()) {
			return defn.getEmptyString();
		} else {
			String unsafeValue = getLiteralValue().toString();
			return defn.beginStringValue() + defn.safeString(unsafeValue) + defn.endStringValue();
		}
	}

	@Override
	protected String getFromResultSet(DBDefinition database, ResultSet resultSet, String fullColumnName) throws SQLException {
		String gotString = resultSet.getString(fullColumnName);
		if (!database.supportsDifferenceBetweenNullAndEmptyString()) {
			if (gotString != null && gotString.isEmpty()) {
				return null;
			}
		}
		return gotString;
	}
	
	@Override
	public UntypedColumn getColumn(RowDefinition row) throws IncorrectRowProviderInstanceSuppliedException {
		return new UntypedColumn(row, this);
	}

	@Override
	public boolean isAggregator() {
		return false;
	}

	@Override
	public DBUntypedValue copy() {
		return (DBUntypedValue) super.copy();
	}

	@Override
	public boolean getIncludesNull() {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public StringExpression stringResult() {
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}
	
}
