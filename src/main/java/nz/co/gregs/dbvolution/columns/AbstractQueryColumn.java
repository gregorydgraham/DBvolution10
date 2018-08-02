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

import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import nz.co.gregs.dbvolution.query.RowDefinition;


public class AbstractQueryColumn extends AbstractColumn {
	
	private static final long serialVersionUID = 1l;

	private final DBQuery query;
	private final QueryableDatatype<?> field;
	private boolean useTableAlias = true;

	/**
	 * Creates an AbstractColumn representing a table and column.
	 *
	 * <p>
	 * Stores the RowDefinition (generally a DBRow subclass) and a field of the
	 * RowDefinition so that the original association can be rebuilt where the
	 * expression is converted into SQL.
	 *
	 * @param query
	 * @param field the field of the row that represents the database column
	 * @throws IncorrectRowProviderInstanceSuppliedException Please note the the
	 * field must be a field of the row
	 */
	public AbstractQueryColumn(DBQuery query, QueryableDatatype<?> field) throws IncorrectRowProviderInstanceSuppliedException {
		super(null, field);
		this.query = query;
		this.field = field;
	}

	@Override
	public String toSQLString(DBDefinition db) {
		if (field.hasColumnExpression()) {
			DBExpression[] columnExpressions = field.getColumnExpression();
			StringBuilder toSQLString = new StringBuilder();
			for (DBExpression columnExpression : columnExpressions) {
				toSQLString.append(columnExpression.toSQLString(db));
			}
			return toSQLString.toString();
		}
		throw new RuntimeException("AbstractQueryColumn does not have a column expression");
	}

	@Override
	public AbstractQueryColumn copy() {
		return new AbstractQueryColumn(query, field.copy());
//		try {
//			Constructor<? extends AbstractQueryColumn> constructor = this.getClass().getConstructor(DBQuery.class, QueryableDatatype.class);
//			AbstractQueryColumn newInstance = constructor.newInstance(getQuery(), getField());
//			return newInstance;
//		} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
//			throw new DBRuntimeException("Unable To Copy " + this.getClass().getSimpleName() + ": please ensure it has a public " + this.getClass().getSimpleName() + "(DBRow, Object) constructor.", ex);
//		}
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the propertyWrapperOfQDT
	 */
	@Override
	public PropertyWrapper getPropertyWrapper() {
		throw new RuntimeException("AbstractQueryColumn has no PropertyWrapper to return");
	}

	/**
	 * Wrap this column in the equivalent DBValue subclass.
	 *
	 * <p>
	 * Probably this should be implemented as:<br>
	 * public MyValue asExpression(){return new MyValue(this);}
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return this instance as a StringValue, NumberValue, DateValue, or
	 * LargeObjectValue as appropriate
	 */
	@Override
	public DBExpression asExpression() {
		return this;
	}

	@Override
	public QueryableDatatype<?> getQueryableDatatypeForExpressionValue() {
		return QueryableDatatype.getQueryableDatatypeForObject(getField());
	}

	@Override
	public boolean isAggregator() {
		boolean aggregator = false;
		if (field.hasColumnExpression()) {
			DBExpression[] columnExpressions = field.getColumnExpression();
			for (DBExpression columnExpression : columnExpressions) {
				aggregator = aggregator || columnExpression.isAggregator();
			}
		}
		return aggregator;
	}

	@Override
	public boolean isPurelyFunctional() {
		return getTablesInvolved().isEmpty();
	}

	@Override
	public Set<DBRow> getTablesInvolved() {
		final HashSet<DBRow> hashSet = new HashSet<>();
		hashSet.addAll(query.getAllTables());
		return hashSet;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the dbrow
	 */
	protected DBQuery getQuery() {
		return query;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the field
	 */
	@Override
	protected QueryableDatatype<?> getField() {
		return field;
	}

	/**
	 * Gets the DBvolution-centric value of the column for the instance of
	 * RowDefinition (DBRow/DBreport) supplied.
	 *
	 * <p>
	 * The value returned may have undergone type conversion from the target
	 * object's actual property type, if a type adaptor is present.
	 *
	 * @param row resolve the column for this row and provide the
	 * QueryableDatatype that is appropriate
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the QDT version of the field on the DBRow
	 */
	@Override
	public QueryableDatatype<?> getAppropriateQDTFromRow(RowDefinition row) {
		throw new RuntimeException("AbstractQueryColumn.getAppropriateQDTFromRow: does not have connect to a DBRow");
	}

	/**
	 * Gets the value of the declared column in the RowDefinition/DBRow/DBReport
	 * supplied, prior to type conversion to the DBvolution-centric type.
	 *
	 * <p>
	 * you should probably be using {@link #getAppropriateQDTFromRow(nz.co.gregs.dbvolution.query.RowDefinition)
	 * }
	 *
	 * @param row resolve the column for this row and provide the appropriate Java
	 * field (may be a QueryableDatatype)
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the actual field on the DBRow object referenced by this column.
	 */
	@Override
	public Object getAppropriateFieldFromRow(RowDefinition row) {
		throw new RuntimeException("AbstractQueryColumn.getAppropriateFieldFromRow: does not have connect to a DBRow");
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the useTableAlias
	 */
	@Override
	protected boolean isUseTableAlias() {
		return useTableAlias;
	}

	/**
	 * @param useTableAlias the useTableAlias to set
	 */
	@Override
	protected void setUseTableAlias(boolean useTableAlias) {
		this.useTableAlias = useTableAlias;
	}

	/**
	 * Returns a new version of the DBRow from which this column has been made.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return an appropriate DBRow
	 */
	@SuppressWarnings("unchecked")
	@Override
	public DBRow getInstanceOfRow() {
		throw new RuntimeException("AbstractQueryColumn.getInstanceOfRow: does not have connect to a DBRow");
	}

	/**
	 * Returns the class of the DBRow from which this column has been made.
	 *
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return an appropriate DBRow class
	 */
	@Override
	public Class<? extends DBRow> getClassReferencedByForeignKey() {
		throw new RuntimeException("AbstractQueryColumn.getClassReferencedByForeignKey: does not have connect to a DBRow");
	}

	@Override
	public boolean getSortDirection() {
		return field.getSortOrder();
	}
}
