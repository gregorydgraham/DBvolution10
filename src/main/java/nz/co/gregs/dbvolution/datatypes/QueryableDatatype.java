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
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.actions.DBActionList;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException;
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
import nz.co.gregs.dbvolution.query.RowDefinition;
import nz.co.gregs.dbvolution.results.IntegerResult;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 * @param <T>
 */
public abstract class QueryableDatatype<T> extends Object implements Serializable, DBExpression {

	private static final long serialVersionUID = 1L;
	private T literalValue = null;
	private boolean isDBNull = false;
	private DBOperator operator = null;
	private boolean undefined = true;
	private boolean changed = false;
	private QueryableDatatype<T> previousValueAsQDT = null;

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
	transient PropertyWrapperDefinition propertyWrapperDefn; // no guarantees whether this gets set
	private DBExpression[] columnExpression = new DBExpression[]{};
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
	protected QueryableDatatype(T obj) {
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
	protected QueryableDatatype(DBExpression[] columnExpression) {
		this.columnExpression = Arrays.copyOf(columnExpression, columnExpression.length);
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
		this.columnExpression = new DBExpression[]{columnExpression};
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a new instance of the supplied QDT class
	 */
	public static <T extends QueryableDatatype<?>> T getQueryableDatatypeInstance(Class<T> requiredQueryableDatatype) {
		try {
			return requiredQueryableDatatype.getConstructor().newInstance();
		} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
			throw new RuntimeException("Unable To Create " + requiredQueryableDatatype.getClass().getSimpleName() + ": Please ensure that the constructor of " + requiredQueryableDatatype.getClass().getSimpleName() + " has no arguments, throws no exceptions, and is public", ex);
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
	 * @param <S> the base datatype returned by the QDT
	 * @param o	the object to be encapsulated in the QDT
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return a QDT that will provide good results for the provided object.
	 */
	@SuppressWarnings("unchecked")
	static public <S extends Object> QueryableDatatype<S> getQueryableDatatypeForObject(S o) {
		QueryableDatatype<S> qdt;
//		if (o instanceof QueryableDatatype) {
//			qdt = QueryableDatatype.getQueryableDatatypeInstance(((QueryableDatatype) o).getClass());
//			qdt.setLiteralValue(((QueryableDatatype) o).getLiteralValue());
//		} else {
		if (o instanceof Integer) {
			qdt = (QueryableDatatype<S>) new DBInteger();
		} else if (o instanceof Long) {
			qdt = (QueryableDatatype<S>) new DBInteger();
		} else if (o instanceof Number) {
			qdt = (QueryableDatatype<S>) new DBNumber();
		} else if (o instanceof String) {
			qdt = (QueryableDatatype<S>) new DBString();
		} else if (o instanceof Date) {
			qdt = (QueryableDatatype<S>) new DBDate();
		} else if (o instanceof Byte[]) {
			qdt = (QueryableDatatype<S>) new DBLargeBinary();
		} else if (o instanceof Boolean) {
			qdt = (QueryableDatatype<S>) new DBBoolean();
		} else if (o instanceof IntegerResult) {
			qdt = (QueryableDatatype<S>) new DBInteger();
		} else if (o instanceof NumberResult) {
			qdt = (QueryableDatatype<S>) new DBNumber();
		} else if (o instanceof StringResult) {
			qdt = (QueryableDatatype<S>) new DBString();
		} else if (o instanceof DateResult) {
			qdt = (QueryableDatatype<S>) new DBDate();
		} else if (o instanceof LargeObjectResult) {
			qdt = (QueryableDatatype<S>) new DBLargeBinary();
		} else if (o instanceof BooleanResult) {
			qdt = (QueryableDatatype<S>) new DBBoolean();
		} else {
			qdt = new DBJavaObject<>();
		}
		qdt.setLiteralValue(o);
//		}
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a complete copy of the QDT with all values set.
	 */
	@Override
	@SuppressWarnings("unchecked")
	public synchronized QueryableDatatype<T> copy() {
		QueryableDatatype<T> newQDT = this;
		try {
			synchronized (newQDT) {
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

				newQDT.sort = this.sort;
				final DBExpression[] columnExpressions = this.getColumnExpression();
				final DBExpression[] newExpressions = new DBExpression[columnExpressions.length];
				int i = 0;
				for (DBExpression columnExpression1 : columnExpressions) {
					newExpressions[i] = columnExpression1.copy();
					i++;
				}
				newQDT.setColumnExpression(newExpressions);
			}
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return this instance.
	 */
	public QueryableDatatype<T> removeConstraints() {
		isDBNull = false;
		this.operator = null;
		return this;
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the literal value, if defined, which may be null
	 */
	public T getValue() {
		if (undefined || isNull()) {
			return null;
		} else {
			return getLiteralValue();
		}
	}

	/**
	 * Gets the current literal value of this queryable data type or the value
	 * supplied if the value is NULL.
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @param valueIfNull
	 * @return the literal value, if defined, which may be null
	 */
	public T getValueWithDefaultValue(T valueIfNull) {
		T value = getValue();
		if (value == null) {
			return valueIfNull;
		} else {
			return value;
		}
	}

	/**
	 * Set the value of this QDT to the value provided.
	 *
	 * @param newLiteralValue the new value
	 */
	void setValue(T newLiteralValue) {
		this.setLiteralValue(newLiteralValue);
	}

	/**
	 * Used by {@link InternalQueryableDatatypeProxy#setValueFromDatabase(java.lang.Object)
	 *
	 * @param newLiteralValue the new value
	 */
	void setValueFromDatabase(T newLiteralValue) {
		this.setLiteralValueInternal(newLiteralValue);
		setValueHasBeenCalled=false;
		changed=false;
		setDefined(true);
	}

	/**
	 * Set the value of this QDT to the value provided from the standard string
	 * encoding of this datatype.
	 *
	 * <p>
	 * A good example of this method is {@link DBBoolean#setValueFromStandardStringEncoding(java.lang.String)
	 * } which translates the string encodings TRUE, YES, and 1 to true.
	 *
	 * <p>
	 * Subclass writers should ensure that the method handles nulls correctly and
	 * throws an exception if an inappropriate value is supplied.
	 *
	 * @param encodedValue the value of the QDT in the appropriate encoding
	 */
	protected abstract void setValueFromStandardStringEncoding(String encodedValue);

	/**
	 * Sets the literal value of this queryable data type. Replaces any assigned
	 * operator with an {@code equals} operator on the given value.
	 *
	 * @param newLiteralValue the literalValue to set
	 */
	protected synchronized void setLiteralValue(T newLiteralValue) {
		setLiteralValueInternal(newLiteralValue);
		this.setHasBeenSet(true);
	}

	private void setLiteralValueInternal(T newLiteralValue) {
		QueryableDatatype.this.moveCurrentValueToPreviousValue(newLiteralValue);
		if (newLiteralValue == null) {
			setToNull();
		} else {
			this.literalValue = newLiteralValue;
			if (newLiteralValue instanceof Date) {
				this.setOperator(new DBEqualsOperator(new DBDate((Date) newLiteralValue)));
			} else if (newLiteralValue instanceof Timestamp) {
				this.setOperator(new DBEqualsOperator(new DBDate((Timestamp) newLiteralValue)));
			} else {
				this.setOperator(new DBEqualsOperator(this.copy()));
			}
		}
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the DBOperator that will be used with this QDT
	 */
	protected synchronized DBOperator setToNull() {
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
	 * Provides the SQL datatype used by default for this type of object.
	 *
	 * <p>
	 * This should be overridden in each subclass</p>
	 *
	 * <p>
	 * Example return value: "VARCHAR(1000)"</p>
	 *
	 * <p>
	 * Database specific datatypes are provided by the DBDefinition in the method
	 * {@link DBDefinition#getDatabaseDataTypeOfQueryableDatatype}</p>
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @param defn
	 * @return the literal value as it would appear in an SQL statement i.e.
	 * {yada} =&gt; 'yada', {1} =&gt; 1 and {} =&gt; NULL
	 */
	@Override
	public final String toSQLString(DBDefinition defn) {
		if (this.isDBNull || getLiteralValue() == null) {
			return defn.getNull();
		} else if (getLiteralValue() instanceof DBExpression) {
			return "(" + ((DBExpression) getLiteralValue()).toSQLString(defn) + ")";
		} else {
			return formatValueForSQLStatement(defn);
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the literal value translated to a String ready to insert into an
	 * SQL statement
	 */
	protected abstract String formatValueForSQLStatement(DBDefinition db);

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * @param defn database
	 * @param resultSet resultSet
	 * @param resultSetColumnName resultSetColumnName
	 * @throws java.sql.SQLException Database exceptions may be thrown
	 */
	public void setFromResultSet(DBDefinition defn, ResultSet resultSet, String resultSetColumnName) throws SQLException {
		removeConstraints();
		if (resultSet == null || resultSetColumnName == null) {
			this.setToNull(defn);
		} else {
			T dbValue;
			try {
				dbValue = getFromResultSet(defn, resultSet, resultSetColumnName);
				if (resultSet.wasNull()) {
					dbValue = null;
				}
			} catch (SQLException ex) {
				// Probably means the column wasn't selected.
				dbValue = null;
			}
			if (dbValue == null) {
				this.setToNull(defn);
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the expected object from the ResultSet. 1 Database exceptions may
	 * be thrown
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	abstract protected T getFromResultSet(DBDefinition database, ResultSet resultSet, String fullColumnName) throws SQLException;

	private synchronized void moveCurrentValueToPreviousValue(T newLiteralValue) {
		if ((this.isDBNull && newLiteralValue != null)
				|| (getLiteralValue() != null && (newLiteralValue == null || !newLiteralValue.equals(literalValue)))) {
			changed = true;
			@SuppressWarnings("unchecked")
			QueryableDatatype<T> copyOfOldValues = QueryableDatatype.getQueryableDatatypeInstance(this.getClass());
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the previous value of this field as an SQL formatted String
	 */
	public String getPreviousSQLValue(DBDefinition db) {
		QueryableDatatype<T> prevQDT = getPreviousValue();
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return this object
	 */
	private QueryableDatatype<T> setSortOrder(Boolean order) {
		sort = order;
		return this;
	}

	/**
	 * Used to switch the direction of the column's sort order
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return this object
	 */
	public QueryableDatatype<T> setSortOrderAscending() {
		return this.setSortOrder(true);
	}

	/**
	 * Used to switch the direction of the column's sort order
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return this object
	 */
	public QueryableDatatype<T> setSortOrderDescending() {
		return this.setSortOrder(false);
	}

	/**
	 * Return the order in which this QDT will be sorted.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return {@link #SORT_DESCENDING} if the column is to be sorted descending,
	 * {@link #SORT_ASCENDING} otherwise.
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return this instance
	 */
	public QueryableDatatype<T> clear() {
		return removeConstraints();
	}

	@Override
	public int hashCode() {
		return super.hashCode();
	}

	@Override
	public boolean equals(Object otherObject) {
		if (super.equals(otherObject)) {
			return true;
		} else if (otherObject instanceof QueryableDatatype) {
			QueryableDatatype<?> other = (QueryableDatatype<?>) otherObject;
			if (this.operator == null && other.operator == null) {
				if (this.columnExpression.length > 1 && this.columnExpression.length == other.columnExpression.length) {
					for (int i = 0; i < columnExpression.length; i++) {
						if (!this.columnExpression[i].equals(other.columnExpression[i])) {
							return false;
						}
					}
					// all the column expressions match so it must be good
					return true;
				} else {
					if (this.columnExpression.length == 0) {
						return this.getLiteralValue().equals(other.getLiteralValue());
					} else {
						return false;
					}
				}
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the undefined
	 */
	public boolean isDefined() {
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
	@SuppressWarnings("unchecked")
	public QueryableDatatype<T> getQueryableDatatypeForExpressionValue() {
		try {
			final QueryableDatatype<T> newInstance = this.getClass().newInstance();
			newInstance.setColumnExpression(this.getColumnExpression());
			return newInstance;
		} catch (InstantiationException | IllegalAccessException e) {
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the underlying expression if there is one, or NULL otherwise.
	 */
	public final DBExpression[] getColumnExpression() {
		return columnExpression.clone();
	}

	/**
	 * Tests for the expression underlying this QDT or returns FALSE.
	 *
	 * <p>
	 * When the QDT is created using an expression , this method makes the
	 * expression accessible.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if there is a underlying expression, or FALSE otherwise.
	 */
	public final boolean hasColumnExpression() {
		return columnExpression.length > 0;
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		if (hasColumnExpression()) {
			HashSet<DBRow> hashSet = new HashSet<DBRow>();
			for (DBExpression dBExpression : columnExpression) {
				hashSet.addAll(dBExpression.getTablesInvolved());
			}
			return hashSet;
		}
		return new HashSet<DBRow>();
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * Indicates whether this column has had it's value set, either by the
	 * external program or DBV's internal processes.
	 *
	 * @return the setValue method has been called
	 */
	public synchronized boolean hasBeenSet() {
		return setValueHasBeenCalled;
	}

	/**
	 * @param hasBeenSet the setValueHasBeenCalled to set
	 */
	private synchronized void setHasBeenSet(boolean hasBeenSet) {
		this.setValueHasBeenCalled = hasBeenSet;
		this.setChanged(true);
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the literalValue
	 */
	protected synchronized T getLiteralValue() {
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the DBOperator that will be used with this QDT
	 */
	protected DBOperator setToNull(DBDefinition database) {
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
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the previous value of this QDT.
	 */
	protected QueryableDatatype<T> getPreviousValue() {
		return previousValueAsQDT;
	}

	/**
	 * Used internally.
	 *
	 * @param queryableDatatype	queryableDatatype
	 */
	protected void setPreviousValue(QueryableDatatype<T> queryableDatatype) {
		this.previousValueAsQDT = queryableDatatype;
	}

	/**
	 * Used internally.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
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
	protected final void setColumnExpression(DBExpression... columnExpression) {
		this.columnExpression = Arrays.copyOf(columnExpression, columnExpression.length);
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
			for (DBExpression dBExpression : columnExpression) {
				if (!dBExpression.isPurelyFunctional()) {
					return false;
				}
			}
		}
		return true;
	}

	/**
	 *
	 * Returns the column of the object formatted for the database.
	 *
	 * <p>
	 * This method provides a route to transforming all calls to a column prior to
	 * use in SQL.</p>
	 *
	 * <p>
	 * See
	 * {@link DBStringTrimmed#formatColumnForSQLStatement(nz.co.gregs.dbvolution.databases.definitions.DBDefinition, java.lang.String) the implementation in DBStringTrimmed}
	 * for an example.</p>
	 *
	 * @param db	db
	 * @param formattedColumnName the name of the database column or similar
	 * expression ready to be used in an SQL excerpt
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the formatted column ready to be used in an SQL statement
	 */
	public String formatColumnForSQLStatement(DBDefinition db, String formattedColumnName) {
		return formattedColumnName;
	}

	/**
	 * Creates a Column Provider suitable to this QDT.
	 *
	 * <p>
	 * Creates a ColumnProvider object of the correct type for this
	 * QueryableDatatype, using this object and the provided row.</p>
	 *
	 * <p>
	 * Used internally to maintain the relationship between QDTs and their
	 * ColumnProvider equivalents.</p>
	 *
	 * @param row
	 * @return a column object appropriate to this datatype based on the object
	 * and the row
	 * @throws IncorrectRowProviderInstanceSuppliedException if this object is not
	 * a field in the row.
	 */
	public abstract ColumnProvider getColumn(RowDefinition row) throws IncorrectRowProviderInstanceSuppliedException;

	@Override
	public final String createSQLForFromClause(DBDatabase database) {
		if (hasColumnExpression()) {
			StringBuilder str = new StringBuilder();
			for (DBExpression expr : getColumnExpression()) {
				if (str.length() > 0) {
					str.append(", ");
				}
				str.append(expr.createSQLForFromClause(database));
			}
			return str.toString();
		} else {
			throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
		}
	}

	@Override
	public final boolean isComplexExpression() {
		if (hasColumnExpression()) {
			DBExpression[] exprs = getColumnExpression();
			for (DBExpression expr : exprs) {
				if (expr.isComplexExpression()) {
					return true;
				}
			}
		}
		return false;
	}

	@Override
	public String createSQLForGroupByClause(DBDatabase database) {
		return "";
	}
}
