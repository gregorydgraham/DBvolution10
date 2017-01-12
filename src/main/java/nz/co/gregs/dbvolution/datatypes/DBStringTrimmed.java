/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.datatypes;

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.expressions.StringExpression;

/**
 * Variant on DBString that automatically truncates the string value, useful
 * when working with MS SQLServer CHAR columns.
 *
 * @author Gregory Graham
 */
public class DBStringTrimmed extends DBString {

	private static final long serialVersionUID = 1L;

	/**
	 * The default constructor for DBStringTrimmed.
	 *
	 * <p>
	 * Creates an unset undefined DBStringTrimmed object.
	 *
	 */
	public DBStringTrimmed() {
	}

	/**
	 * Creates a DBString with the value provided.
	 *
	 * <p>
	 * The resulting DBString will be set as having the value provided but will
	 * not be defined in the database.
	 *
	 * @param string	string
	 */
	public DBStringTrimmed(String string) {
		super(string);
	}

	/**
	 * Creates a column expression with a string result from the expression
	 * provided.
	 *
	 * <p>
	 * Used in {@link DBReport}, and some {@link DBRow}, sub-classes to derive
	 * data from the database prior to retrieval.
	 *
	 * @param stringExpression	stringExpression
	 */
	public DBStringTrimmed(StringExpression stringExpression) {
		super(stringExpression);
	}

	@Override
	public String formatValueForSQLStatement(DBDatabase db) {
		return db.getDefinition().doTrimFunction(super.formatValueForSQLStatement(db));
	}

	@Override
	public String formatColumnForSQLStatement(DBDatabase db, String formattedColumnName) {
		return db.getDefinition().doTrimFunction(formattedColumnName);
	}

}
