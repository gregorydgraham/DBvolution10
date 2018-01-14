/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.datatypes;

import java.sql.ResultSet;
import java.sql.SQLException;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.expressions.AnyExpression;
import nz.co.gregs.dbvolution.expressions.EqualExpression;
import nz.co.gregs.dbvolution.expressions.IntegerExpression;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapperDefinition;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class DBStatistics extends DBString {

	private static final long serialVersionUID = 1;

	private final EqualExpression originalExpression;
	private final DBString numberProxy = new DBString();;
	private Number countOfRows;
//	private Number rankingHighIsFirst;  // Should be an expression column on an ordinary table
//	private Number rankingLowIsFirst; // Should be an expression column on an ordinary table
	private Number modeSimple;
	private Number modeStrict;
	private Number median;
	private Number firstQuartileValue;
	private Number thirdQuartileValue;
	private transient IntegerExpression countExpr;
	private transient NumberExpression modeSimpleExpression;
	private transient NumberExpression modeStrictExpression;
	private transient NumberExpression medianExpression;
	private transient NumberExpression firstQuartileExpression;
	private transient NumberExpression thirdQuartileExpression;

	/**
	 * The default constructor for DBNumberStatistics.
	 *
	 * <p>
	 * Creates an unset undefined DBNumber object.</p>
	 *
	 * <p>
	 * Use {@link #DBNumberStatistics(nz.co.gregs.dbvolution.expressions.NumberExpression)
	 * } instead.</p>
	 *
	 */
	public DBStatistics() {
		originalExpression = null;
	}

	/**
	 * Creates a column expression with a statistics result from the expression
	 * provided.
	 *
	 * <p>
	 * Used in {@link DBReport}, and some {@link DBRow}, sub-classes to derive
	 * data from the database prior to retrieval.
	 *
	 * @param expressionToGenerateStatsFrom numberExpression
	 */
	public DBStatistics(EqualExpression<?,?,?> expressionToGenerateStatsFrom) {
		super(expressionToGenerateStatsFrom.stringResult());
		this.originalExpression = expressionToGenerateStatsFrom;
		countExpr = IntegerExpression.count(originalExpression);

		this.setColumnExpression(new AnyExpression<?, ?, ?>[]{
			countExpr
		});

	}

	/**
	 * Count of the rows included in this set of statistics
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the size of the statistics collection
	 */
	public Number count() {
		return this.countOfRows;
	}

	/**
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the minimum (smallest) value in this grouping
	 */
	public Number modeSimple() {
		return this.modeSimple;
	}

	/**
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the maximum (largest) value in this grouping
	 */
	public Number modeStrict() {
		return this.modeStrict;
	}

	/**
	 * The median value, that is the value that occurs halfway through the distribution.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the ranking
	 */
	public Number median() {
		return this.median;
	}

	/**
	 * Returns the value that occurs 1/4 of the way through the distribution.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the middle number between the median and the smallest value.
	 */
	public Number firstQuartile() {
		return this.firstQuartileValue;
	}

	/**
	 * Returns the value that occurs 3/4 of the way through the distribution.
	 * 
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the middle number between the median and the largest value.
	 */
	public Number thirdQuartile() {
		return this.thirdQuartileValue;
	}

	@Override
	public String getSQLDatatype() {
		return numberProxy.getSQLDatatype();
	}

	@Override
	public boolean isAggregator() {
		return numberProxy.isAggregator();
	}

	@Override
	public DBStatistics copy() {
		DBStatistics copy = (DBStatistics) super.copy();
		copy.countOfRows = this.countOfRows;
		copy.firstQuartileValue = this.firstQuartileValue;
		copy.modeStrict = this.modeStrict;
		copy.modeSimple = this.modeSimple;
		copy.thirdQuartileValue = this.thirdQuartileValue;
		copy.median = this.median;
		return copy;
	}

	protected Number getFromResultSet(DBDefinition database, ResultSet resultSet, String fullColumnName, int offset) throws SQLException {
		int columnIndex = resultSet.findColumn(fullColumnName) + offset;
		try {
			return resultSet.getBigDecimal(columnIndex);
		} catch (SQLException ex) {
			try {
				return resultSet.getDouble(columnIndex);
			} catch (SQLException ex2) {
				try {
					return resultSet.getLong(columnIndex);
				} catch (SQLException ex3) {
					return null;
				}
			}
		}
	}

	@Override
	public DBStatistics getQueryableDatatypeForExpressionValue() {
		return new DBStatistics();
	}

	@Override
	public void setFromResultSet(DBDefinition database, ResultSet resultSet, String resultSetColumnName) throws SQLException {
		removeConstraints();
		if (resultSet == null || resultSetColumnName == null) {
			this.setToNull();
		} else {
			String dbValue;
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
				PropertyWrapperDefinition propertyWrapperDefinition = getPropertyWrapperDefinition();
				if (propertyWrapperDefinition != null && propertyWrapperDefinition.allColumnAspects != null) {
					final String countColumnAlias = propertyWrapperDefinition.allColumnAspects.get(0).columnAlias;
//					final String modeSimpleAlias = propertyWrapperDefinition.allColumnAspects.get(1).columnAlias;
//					final String modeStrictAlias = propertyWrapperDefinition.allColumnAspects.get(2).columnAlias;
					countOfRows = new DBInteger().getFromResultSet(database, resultSet, countColumnAlias);
//					modeStrict = getFromResultSet(database, resultSet, maxColumnAlias);
//					modeSimple = getFromResultSet(database, resultSet, minColumnAlias);
				} else {
					countOfRows = getFromResultSet(database, resultSet, resultSetColumnName, 0);
//					modeStrict = getFromResultSet(database, resultSet, resultSetColumnName, 1);
//					modeSimple = getFromResultSet(database, resultSet, resultSetColumnName, 2);
				}
				this.setLiteralValue(dbValue);
			}
		}
		setUnchanged();
		setDefined(true);
		propertyWrapperDefn = null;
	}

	@Override
	public String toString() {
		return ("count=" + countOfRows );
	}

}
