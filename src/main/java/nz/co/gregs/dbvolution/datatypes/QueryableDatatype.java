/*
 * Copyright 2013 Gregory Graham.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nz.co.gregs.dbvolution.datatypes;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.UnableInstantiateQueryableDatatypeException;
import nz.co.gregs.dbvolution.exceptions.UnableToCopyQueryableDatatypeException;
import nz.co.gregs.dbvolution.results.BooleanResult;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.results.DateResult;
import nz.co.gregs.dbvolution.results.LargeObjectResult;
import nz.co.gregs.dbvolution.results.NumberResult;
import nz.co.gregs.dbvolution.results.StringResult;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapperDefinition;
import nz.co.gregs.dbvolution.operators.DBEqualsOperator;
import nz.co.gregs.dbvolution.operators.DBIsNullOperator;
import nz.co.gregs.dbvolution.operators.DBOperator;

/**
 *
 * @author Gregory Graham
 */
public abstract class QueryableDatatype extends Object implements Serializable, DBExpression {

	private static final long serialVersionUID = 1L;
	Object literalValue = null;
	private boolean isDBNull = false;
	private DBOperator operator = null;
	private boolean undefined = true;
	private boolean changed = false;
	private QueryableDatatype previousValueAsQDT = null;

	/**
	 * Used to indicate the the QDT should be sorted so that the values run from
	 * A-&gt;Z or 0-&gt;9 when using the {@link #setSortOrder(java.lang.Boolean)
	 * }
	 * method.
	 */
	public final static Boolean SORT_ASCENDING = Boolean.TRUE;

	/**
	 * Used to indicate the the QDT should be sorted so that the values run from
	 * Z-&gt;A or 9-&gt;0 when using the {@link #setSortOrder(java.lang.Boolean)
	 * }
	 * method.
	 */
	public final static Boolean SORT_DESCENDING = Boolean.FALSE;
	private Boolean sort = SORT_ASCENDING;
	transient private PropertyWrapperDefinition propertyWrapperDefn; // no guarantees whether this gets set
	private DBExpression columnExpression = null;
	private boolean setValueHasBeenCalled = false;

	/**
	 * Default Constructor
	 *
	 */
	protected QueryableDatatype() {
	}

	/**
	 * Create a QueryableDatatype with the exact value provided.
	 *
	 * <p>
	 * Equivalent to {@code new QueryableDatatype().setValue(obj);}
	 *
	 * @param obj the literal value of the QDT.
	 */
	protected QueryableDatatype(Object obj) {
		if (obj == null) {
			this.isDBNull = true;
		} else {
			this.literalValue = obj;
			this.operator = new DBEqualsOperator(this);
			undefined = false;
		}
	}

	/**
	 * Create a QDT with a permanent column expression.
	 *
	 * <p>
	 * Use this method within a DBRow sub-class to create a column that uses an
	 * expression to create the value at query time.
	 *
	 * <p>
	 * This is particularly useful for trimming strings or converting between
	 * types but also allows for complex arithmetic and transformations.
	 *
	 * @param columnExpression	columnExpression
	 */
	protected QueryableDatatype(DBExpression columnExpression) {
		this.columnExpression = columnExpression.copy();
	}

	/**
	 * Factory method that creates a new QDT instance with the same class as the
	 * provided example.
	 *
	 * <p>
	 * This method only provides a new blank instance. To copy the QDT and its
	 * fields, use {@link #copy() }.
	 *
	 * @param <T> the QDT type
	 * @param requiredQueryableDatatype requiredQueryableDatatype
	 * @return a new instance of the supplied QDT class
	 */
	public static <T extends QueryableDatatype> T getQueryableDatatypeInstance(Class<T> requiredQueryableDatatype) {
		try {
			return requiredQueryableDatatype.getConstructor().newInstance();
		} catch (NoSuchMethodException ex) {
			throw new RuntimeException("Unable To Create " + requiredQueryableDatatype.getClass().getSimpleName() + ": Please ensure that the constructor of  " + requiredQueryableDatatype.getClass().getSimpleName() + " has no arguments, throws no exceptions, and is public", ex);
		} catch (SecurityException ex) {
			throw new RuntimeException("Unable To Create " + requiredQueryableDatatype.getClass().getSimpleName() + ": Please ensure that the constructor of  " + requiredQueryableDatatype.getClass().getSimpleName() + " has no arguments, throws no exceptions, and is public", ex);
		} catch (InstantiationException ex) {
			throw new RuntimeException("Unable To Create " + requiredQueryableDatatype.getClass().getSimpleName() + ": Please ensure that the constructor of  " + requiredQueryableDatatype.getClass().getSimpleName() + " has no arguments, throws no exceptions, and is public", ex);
		} catch (IllegalAccessException ex) {
			throw new RuntimeException("Unable To Create " + requiredQueryableDatatype.getClass().getSimpleName() + ": Please ensure that the constructor of  " + requiredQueryableDatatype.getClass().getSimpleName() + " has no arguments, throws no exceptions, and is public", ex);
		} catch (IllegalArgumentException ex) {
			throw new RuntimeException("Unable To Create " + requiredQueryableDatatype.getClass().getSimpleName() + ": Please ensure that the constructor of  " + requiredQueryableDatatype.getClass().getSimpleName() + " has no arguments, throws no exceptions, and is public", ex);
		} catch (InvocationTargetException ex) {
			throw new RuntimeException("Unable To Create " + requiredQueryableDatatype.getClass().getSimpleName() + ": Please ensure that the constructor of  " + requiredQueryableDatatype.getClass().getSimpleName() + " has no arguments, throws no exceptions, and is public", ex);
		}
	}

	/**
	 * Returns an appropriate QueryableDatatype for the provided object.
	 *
	 * <p>
	 * Provides the base QDTs for Integer, Number, String, Date, Byte[], Boolean,
	 * NumberResult, StringResult, DateResult, LargeObjectResult, BooleanResult
	 * and defaults everything else to DBJavaObject.
	 *
	 * @param o	o
	 * @return a QDT that will provide good results for the provided object.
	 */
	static public QueryableDatatype getQueryableDatatypeForObject(Object o) {
		QueryableDatatype qdt;
		if (o instanceof QueryableDatatype) {
			qdt = QueryableDatatype.getQueryableDatatypeInstance(((QueryableDatatype) o).getClass());
			qdt.setLiteralValue(((QueryableDatatype) o).getLiteralValue());
		} else {
			if (o instanceof Integer) {
				qdt = new DBInteger();
			} else if (o instanceof Long) {
				qdt = new DBInteger();
			} else if (o instanceof Number) {
				qdt = new DBNumber();
			} else if (o instanceof String) {
				qdt = new DBString();
			} else if (o instanceof Date) {
				qdt = new DBDate();
			} else if (o instanceof Byte[]) {
				qdt = new DBByteArray();
			} else if (o instanceof Boolean) {
				qdt = new DBBoolean();
			} else if (o instanceof NumberResult) {
				qdt = new DBNumber();
			} else if (o instanceof StringResult) {
				qdt = new DBString();
			} else if (o instanceof DateResult) {
				qdt = new DBDate();
			} else if (o instanceof LargeObjectResult) {
				qdt = new DBByteArray();
			} else if (o instanceof BooleanResult) {
				qdt = new DBBoolean();
			} else {
				qdt = new DBJavaObject<Object>();
			}
			qdt.setLiteralValue(o);
		}
		return qdt;
	}

	/**
	 * Copies a QueryableDatatype and returns the copy.
	 *
	 * Used internally to provide immutability to DBOperator objects.
	 *
	 * The intention is that this method will provide a snapshot of the QDT at
	 * this moment in time and copy or clone any internal objects that might
	 * change.
	 *
	 * Subclasses should extend this method if they have fields that maintain the
	 * state of the QDT.
	 *
	 * Always use the super.copy() method first when overriding this method.
	 *
	 * @return a complete copy of the QDT with all values set.
	 */
	@Override
	public QueryableDatatype copy() {
		QueryableDatatype newQDT = this;
		try {
			newQDT = this.getClass().newInstance();

			newQDT.literalValue = this.getLiteralValue();
			newQDT.isDBNull = this.isDBNull;
			newQDT.operator = this.operator;
			newQDT.undefined = this.undefined;
			newQDT.changed = this.changed;
			newQDT.setValueHasBeenCalled = this.setValueHasBeenCalled;
			if (this.previousValueAsQDT != null) {
				newQDT.previousValueAsQDT = this.previousValueAsQDT.copy();
			}
//			newQDT.isPrimaryKey = this.isPrimaryKey;
			newQDT.sort = this.sort;
			newQDT.setColumnExpression(this.getColumnExpression());
		} catch (InstantiationException ex) {
			throw new UnableInstantiateQueryableDatatypeException(this, ex);
		} catch (IllegalAccessException ex) {
			throw new UnableToCopyQueryableDatatypeException(this, ex);
		}

		return newQDT;
	}

	@Override
	public String toString() {
		return (getLiteralValue() == null ? "" : getLiteralValue().toString());
	}

	/**
	 * Returns the raw value as a String
	 *
	 * <p>
	 * A database NULL is treated as an empty string, use {@link #isNull() } to
	 * handle NULLs separately.
	 *
	 * @return the literal value as a String
	 */
	public String stringValue() {
		return (getLiteralValue() == null ? "" : getLiteralValue().toString());
	}

	/**
	 * Remove the conditions, criteria, and operators applied to this QDT.
	 *
	 * <p>
	 * After calling this method, this object will not cause a where clause to be
	 * generated in any subsequent queries.
	 *
	 * @return this instance.
	 */
	public QueryableDatatype removeConstraints() {
		isDBNull = false;
		this.operator = null;
		return this;
	}
	
	/**
	 *
	 * @param db db
	 * @param column column
	 * @return the section of the SQL query between WHERE and ORDER BY
	 */
	public String getWhereClause(DBDatabase db, ColumnProvider column) {
		return getWhereClauseUsingOperators(db, column);
	}

	private String getWhereClauseUsingOperators(DBDatabase db, ColumnProvider column) {
		String whereClause = "";
		DBOperator op = this.getOperator();
		if (op != null) {
			if (column instanceof DBExpression) {
				DBExpression requiredExpression = (DBExpression) column;
				if (this.hasColumnExpression()) {
					requiredExpression = this.getColumnExpression();
				}
				whereClause = op.generateWhereExpression(db, requiredExpression).toSQLString(db);
			}
		}
		return whereClause;
	}

	/**
	 * Negate the meaning of the comparison associated with this object.
	 *
	 * <p>
	 * For instance, given thisQDT.permittedValue(1), thisQDT.negateOperator()
	 * will cause the operator to return everything other than 1.
	 *
	 * <p>
	 * If this object has an operator defined for it, this method will invert the
	 * meaning of the operator by calling the operator's {@link DBOperator#invertOperator(java.lang.Boolean)
	 * } with "true".
	 */
	public void negateOperator() {
		if (getOperator() != null) {
			getOperator().invertOperator(true);
		} else {
			throw new RuntimeException("No Operator Has Been Defined Yet: please use the permitted/excluded methods before negating the operation");
		}
	}

	/**
	 * Gets the current literal value of this queryable data type. The returned
	 * value <i>should</i> be in the correct type as appropriate for the type of
	 * queryable data type.
	 *
	 * <p>
	 * This method will return NULL if the QDT represents a database NULL OR the
	 * field is undefined. Use {@link #isNull() } and {@link #isDefined() } to
	 * differentiate the 2 states.
	 *
	 * <p>
	 * Undefined QDTs represents a QDT that is not a field from the database.
	 * Undefined QDTs are similar to {@link DBRow#isDefined undefined DBRows}
	 *
	 * @return the literal value, if defined, which may be null
	 */
	public Object getValue() {
		if (undefined || isNull()) {
			return null;
		} else {
			return getLiteralValue();
		}
	}

	/**
	 * Internal method, use the subclasses setValue methods or {@link DBRow#setPrimaryKey(java.lang.Object)
	 * } instead.
	 *
	 * <p>
	 * <b>Set the value of this QDT to the value provided.</b>
	 *
	 * <p>
	 * Subclass writers should ensure that the method handles nulls correctly and
	 * throws an exception if an inappropriate value is supplied.
	 *
	 * <p>
	 * This method is public for internal reasons and you should provide/use
	 * another more strongly typed version of setValue.
	 *
	 *
	 */
	void setValue(Object newLiteralValue) {
		this.setLiteralValue(newLiteralValue);
	}

	/**
	 * Sets the literal value of this queryable data type. Replaces any assigned
	 * operator with an {@code equals} operator on the given value.
	 *
	 * @param newLiteralValue the literalValue to set
	 */
	protected void setLiteralValue(Object newLiteralValue) {
		if (newLiteralValue == null) {
			QueryableDatatype.this.moveCurrentValueToPreviousValue(newLiteralValue);
			setToNull();
		} else {
			QueryableDatatype.this.moveCurrentValueToPreviousValue(newLiteralValue);
			this.literalValue = newLiteralValue;
			if (newLiteralValue instanceof Date) {
				this.setOperator(new DBEqualsOperator(new DBDate((Date) newLiteralValue)));
			} else if (newLiteralValue instanceof Timestamp) {
				this.setOperator(new DBEqualsOperator(new DBDate((Timestamp) newLiteralValue)));
			} else {
				this.setOperator(new DBEqualsOperator(this.copy()));
			}
		}
		this.setHasBeenSet(true);
	}

	/**
	 * Clear the changes to this QDT and remove the previous value as though this
	 * QDT had never had any value other than the current value.
	 *
	 */
	public void setUnchanged() {
		changed = false;
		setPreviousValue(null);
	}

	/**
	 *
	 * Sets the value of this column to DBNull Also changes the operator to
	 * DBIsNullOperator for comparisons
	 *
	 * @return the DBOperator that will be used with this QDT
	 */
	protected DBOperator setToNull() {
		this.literalValue = null;
		this.isDBNull = true;
		this.setOperator(new DBIsNullOperator());
		return getOperator();
	}

	/**
	 * Causes the underlying operator to explicitly include NULL values in it's
	 * processing.
	 *
	 * <p>
	 * For instance: normally thisQDT.permittedValue(1) will only return fields
	 * with the value 1. Calling thisQDT.includingNulls() as well will cause the
	 * operator to return fields with value 1 and those with value NULL.
	 *
	 */
	public void includingNulls() {
		this.operator.includeNulls();
	}

	/**
	 *
	 * Provides the SQL datatype used by default for this type of object
	 *
	 * This should be overridden in each subclass
	 *
	 * Example return value: "VARCHAR(1000)"
	 *
	 * @return the standard SQL datatype that corresponds to this QDT as a String
	 */
	public abstract String getSQLDatatype();

	/**
	 * Formats the literal value for use within an SQL statement.
	 *
	 * <p>
	 * This is used internally to transform the Java object in to SQL format. You
	 * won't need to use it.
	 *
	 *
	 * @return the literal value as it would appear in an SQL statement i.e.
	 * {yada} =&gt; 'yada', {1} =&gt; 1 and {} =&gt; NULL
	 */
	@Override
	public final String toSQLString(DBDatabase db) {
		DBDefinition def = db.getDefinition();
		if (this.isDBNull || getLiteralValue() == null) {
			return def.getNull();
		} else if (getLiteralValue() instanceof DBExpression) {
			return "(" + ((DBExpression) getLiteralValue()).toSQLString(db) + ")";
		} else {
			return formatValueForSQLStatement(db);
		}
	}

	/**
	 *
	 * Returns the value of the object formatted for the database
	 *
	 * This should be overridden in each subclass
	 *
	 * This method is called by toSQLString after checking for NULLs and should
	 * return a string representation of the object formatted for use within a SQL
	 * select, insert, update, or delete statement.
	 *
	 * For Example:
	 *
	 * DBString{yada} =&gt; 'yada'
	 *
	 * DBInteger{1234} =&gt; 123
	 *
	 * DBDate{1/March/2013} =&gt; TO_DATE('20130301', 'YYYYMMDD')
	 *
	 * @param db	db
	 * @return the literal value translated to a String ready to insert into an
	 * SQL statement
	 */
	protected abstract String formatValueForSQLStatement(DBDatabase db);

	/**
	 * @return the operator
	 */
	public DBOperator getOperator() {
		return operator;
	}

	/**
	 * @param operator the operator to set
	 */
	public void setOperator(DBOperator operator) {
		removeConstraints();
		this.operator = operator;
		if (undefined) {
			undefined = false;
		} else {
			changed = true;
		}
	}

	/**
	 * Indicates that the value of this QDT has been changed from its defined
	 * value.
	 *
	 * @return TRUE if the set value of this QDT has been changed since it was
	 * retrieved or updated, otherwise FALSE.
	 */
	public boolean hasChanged() {
		return changed;
	}

	/**
	 * Used internally to set the QDT to the value returned from the database.
	 *
	 * <p>
	 * If you create a new QDT you should override this method. The default
	 * implementation in {@link QueryableDatatype} processes the ResultSet column
	 * as a String. You should follow the basic pattern but change
	 * {@link ResultSet#getString(java.lang.String) ResultSet.getString(String)}
	 * to the required ResultSet method and add any required post-processing.
	 *
	 * <p>
	 * Note that most of the method is dedicated to detecting NULL values. This is
	 * very important as are the calls to {@link #setUnchanged() } and {@link #setDefined(boolean)
	 * }
	 *
	 * @param database database
	 * @param resultSet resultSet
	 * @param resultSetColumnName resultSetColumnName
	 * @throws java.sql.SQLException Database exceptions may be thrown
	 */
	public void setFromResultSet(DBDatabase database, ResultSet resultSet, String resultSetColumnName) throws SQLException {
		removeConstraints();
		if (resultSet == null || resultSetColumnName == null) {
			this.setToNull();
		} else {
			Object dbValue;
			try {
				dbValue = getFromResultSet(database, resultSet, resultSetColumnName);
				if (resultSet.wasNull()) {
					dbValue = null;
				}
			} catch (SQLException ex) {
				// Probably means the column wasn't selected.
				dbValue = null;
			}
			if (dbValue == null) {
				this.setToNull(database);
			} else {
				this.setLiteralValue(dbValue);
			}
		}
		setUnchanged();
		setDefined(true);
		propertyWrapperDefn = null;
	}

	/**
	 * Returns the correct object from the ResultSet for the QueryableDatatype to
	 * handle.
	 *
	 * @param database database
	 * @param resultSet resultSet
	 * @param fullColumnName fullColumnName
	 * @return the expected object from the ResultSet. 1 Database exceptions may
	 * be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	abstract protected Object getFromResultSet(DBDatabase database, ResultSet resultSet, String fullColumnName) throws SQLException;

	private void moveCurrentValueToPreviousValue(Object newLiteralValue) {
		if ((this.isDBNull && newLiteralValue != null)
				|| (getLiteralValue() != null && (newLiteralValue == null || !newLiteralValue.equals(literalValue)))) {
			changed = true;
			QueryableDatatype copyOfOldValues = QueryableDatatype.getQueryableDatatypeInstance(this.getClass());
			if (this.isDBNull) {
				copyOfOldValues.setToNull();
			} else {
				copyOfOldValues.setLiteralValue(this.getLiteralValue());
			}
			setPreviousValue(copyOfOldValues);
		}
	}

	/**
	 * Indicates whether object is NULL within the database
	 *
	 * <p>
	 * Databases and Java both use the term NULL but for slightly different
	 * meanings.
	 *
	 * <p>
	 * This method indicates whether the field represented by this object is NULL
	 * in the database sense.
	 *
	 * @return TRUE if this object represents a NULL database value, otherwise
	 * FALSE
	 */
	public boolean isNull() {
		return isDBNull || getLiteralValue() == null;
	}

	/**
	 * Indicates whether object is NULL within the database
	 *
	 * <p>
	 * Databases and Java both use the term NULL but for slightly different
	 * meanings.
	 *
	 * <p>
	 * This method indicates whether the field represented by this object is NULL
	 * in the database sense.
	 *
	 * @return TRUE if this object represents a NULL database value, otherwise
	 * FALSE
	 */
	public boolean isNotNull() {
		return !isNull();
	}

	/**
	 * Returns the previous value of this field as an SQL formatted String.
	 *
	 * <p>
	 * Used by {@link DBActionList} to generate
	 * {@link DBActionList#getRevertActionList() revert action lists}.
	 *
	 * @param db	db
	 * @return the previous value of this field as an SQL formatted String
	 */
	public String getPreviousSQLValue(DBDatabase db) {
		QueryableDatatype prevQDT = getPreviousValue();
		return (prevQDT == null) ? null : prevQDT.toSQLString(db);
	}

	/**
	 * Used to switch the direction of the column's sort order
	 *
	 * use setSortOrderAscending() and setSortOrderDescending() where possible
	 *
	 * use setSortOrderAscending() and setSortOrderDescending() where possible
	 *
	 * Use Boolean.TRUE for Ascending Use Boolean.FALSE for Descending
	 *
	 *
	 * @return this object
	 */
	private QueryableDatatype setSortOrder(Boolean order) {
		sort = order;
		return this;
	}

	/**
	 * Used to switch the direction of the column's sort order
	 *
	 * @return this object
	 */
	public QueryableDatatype setSortOrderAscending() {
		return this.setSortOrder(true);
	}

	/**
	 * Used to switch the direction of the column's sort order
	 *
	 * @return this object
	 */
	public QueryableDatatype setSortOrderDescending() {
		return this.setSortOrder(false);
	}

	/**
	 * Return the order in which this QDT will be sorted.
	 *
	 * @return {@link #SORT_ASCENDING} if the column is to be sorted ascending,
	 * {@link #SORT_DESCENDING} otherwise.
	 */
	public Boolean getSortOrder() {
		return sort;
	}

	/**
	 * Remove the conditions, criteria, and operators applied to this QDT.
	 *
	 * <p>
	 * After calling this method, this object will not cause a where clause to be
	 * generated in any subsequent queries.
	 *
	 * <p>
	 * Synonym for {@link #removeConstraints() }.
	 *
	 * @return this instance
	 */
	public QueryableDatatype clear() {
		return removeConstraints();
	}

	@Override
	public int hashCode() {
		return super.hashCode();
	}

	@Override
	public boolean equals(Object otherObject) {
		if (otherObject instanceof QueryableDatatype) {
			QueryableDatatype other = (QueryableDatatype) otherObject;
			if (other == null) {
				return false;
			} else if (this.operator == null && other.operator == null) {
				return this.getLiteralValue().equals(other.getLiteralValue());
//			return true;
			} else if (this.operator != null && other.operator == null) {
				return false;
			} else if (this.operator == null && other.operator != null) {
				return false;
			} else {
				return this.getOperator().equals(other.getOperator());
			}
		} else {
			return false;
		}
	}

	/**
	 * @return the undefined
	 */
	protected boolean isDefined() {
		return !undefined;
	}

	/**
	 * @param defined the undefined to set
	 */
	protected void setDefined(boolean defined) {
		this.undefined = !defined;
	}

	/**
	 * Sets the internal reference the property wrapper of the field or bean
	 * property that references this QueryableDatatype. Supports QDT types that
	 * need extra meta-information, such as the {@code DBEnum} type.
	 *
	 * <p>
	 * Called by the property wrapper itself when it gets or sets the field, so
	 * this QDT's reference to its owning field is populated 99% of the time.
	 *
	 * <p>
	 * Can't be called directly, must be called via
	 * {@link InternalQueryableDatatypeProxy}.
	 *
	 * <p>
	 * <i>Thread-safety: relatively safe, as PropertyWrappers are thread-safe and
	 * interchangeable.</i>
	 *
	 *
	 */
	void setPropertyWrapper(PropertyWrapperDefinition propertyWrapper) {
		this.propertyWrapperDefn = propertyWrapper;
	}

	@Override
	public QueryableDatatype getQueryableDatatypeForExpressionValue() {
		try {
			return this.getClass().newInstance();
		} catch (InstantiationException e) {
			return this;
		} catch (IllegalAccessException ex) {
			return this;
		}
	}

	/**
	 * Returns the expression underlying this QDT or null.
	 *
	 * <p>
	 * When the QDT is created using an expression , this method makes the
	 * expression accessible.
	 *
	 * @return the underlying expression if there is one, or NULL otherwise.
	 */
	public final DBExpression getColumnExpression() {
		return columnExpression;
	}

	/**
	 * Tests for the expression underlying this QDT or returns FALSE.
	 *
	 * <p>
	 * When the QDT is created using an expression , this method makes the
	 * expression accessible.
	 *
	 * @return TRUE if there is a underlying expression, or FALSE otherwise.
	 */
	public final boolean hasColumnExpression() {
		return columnExpression != null;
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		if (getColumnExpression() != null) {
			return getColumnExpression().getTablesInvolved();
		}
		return new HashSet<DBRow>();
	}

	/**
	 * @return the setValueHasBeenCalled
	 */
	public boolean hasBeenSet() {
		return setValueHasBeenCalled;
	}

	/**
	 * @param hasBeenSet the setValueHasBeenCalled to set
	 */
	private void setHasBeenSet(boolean hasBeenSet) {
		this.setValueHasBeenCalled = hasBeenSet;
	}

	/**
	 * @return the literalValue
	 */
	protected Object getLiteralValue() {
		return literalValue;
	}

	/**
	 * Used during setFromResultSet to set the QDT to a database NULL value.
	 *
	 * <p>
	 * DBDatabase is supplied so that database-specific processing, such as Oracle
	 * empty strings, can be performed.
	 *
	 * <p>
	 * Sets the value of this column to DBNull Also changes the operator to
	 * DBIsNullOperator for comparisons.
	 *
	 * <p>
	 * The default implementation just calls {@link #setToNull() }
	 *
	 * @param database	database
	 * @return the DBOperator that will be used with this QDT
	 */
	protected DBOperator setToNull(DBDatabase database) {
		return setToNull();
	}

	/**
	 * Used internally.
	 *
	 * @param hasChanged	hasChanged
	 */
	protected void setChanged(boolean hasChanged) {
		if (hasChanged) {
			changed = true;
		} else {
			setUnchanged();
		}
	}

	/**
	 * Used internally.
	 *
	 * @return the previous value of this QDT.
	 */
	protected QueryableDatatype getPreviousValue() {
		return previousValueAsQDT;
	}

	/**
	 * Used internally.
	 *
	 * @param queryableDatatype	queryableDatatype
	 */
	protected void setPreviousValue(QueryableDatatype queryableDatatype) {
		this.previousValueAsQDT = queryableDatatype;
	}

	/**
	 * Used internally.
	 *
	 * @return the PropertyWrapperDefinition
	 */
	protected PropertyWrapperDefinition getPropertyWrapperDefinition() {
		return propertyWrapperDefn;
	}

	/**
	 * Used Internally.
	 *
	 * @param columnExpression the columnExpression to set
	 */
	protected void setColumnExpression(DBExpression columnExpression) {
		this.columnExpression = columnExpression;
	}

	/**
	 * Convenient synonym for setValue(null).
	 *
	 */
	public void setValueToNull() {
		this.setLiteralValue(null);
	}

	@Override
	public boolean isPurelyFunctional() {
		if (!hasColumnExpression()) {
			return getTablesInvolved().isEmpty();
		} else {
			return getColumnExpression().isPurelyFunctional();
		}
	}
}
