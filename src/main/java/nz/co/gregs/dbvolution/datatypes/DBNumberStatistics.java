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
import nz.co.gregs.dbvolution.expressions.IntegerExpression;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.expressions.SimpleNumericExpression;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapperDefinition;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class DBNumberStatistics extends DBNumber {

	private static final long serialVersionUID = 1;

	private NumberExpression originalExpression;
	private final DBNumber numberProxy = new DBNumber();
	private Number minNumber;
	private Number maxNumber;
	private Number medianNumber;
	private Number averageNumber;
	private Number stdDev;
	private Number firstQuartileNumber;
	private Number thirdQuartileNumber;
	private Number countOfRows;
	private NumberExpression averageExpression;
	private NumberExpression maxExpr;
	private NumberExpression minExpr;
	private NumberExpression sumExpr;
	private IntegerExpression countExpr;
	private NumberExpression stdDevExpression;
	private Number sumNumber;

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
	public DBNumberStatistics() {
	}

	/**
	 * Creates a column expression with a statistics result from the expression
	 * provided.
	 *
	 * <p>
	 * Used in {@link DBReport}, and some {@link DBRow}, sub-classes to derive
	 * data from the database prior to retrieval.
	 *
	 * @param numberExpressionToGenerateStatsFrom numberExpression
	 */
	public DBNumberStatistics(NumberExpression numberExpressionToGenerateStatsFrom) {
		super();
		averageExpression = numberExpressionToGenerateStatsFrom.sum().dividedBy(NumberExpression.countAll());
		maxExpr = numberExpressionToGenerateStatsFrom.max();
		minExpr = numberExpressionToGenerateStatsFrom.min();
		sumExpr = numberExpressionToGenerateStatsFrom.sum();
		countExpr = IntegerExpression.countAll();
		stdDevExpression = numberExpressionToGenerateStatsFrom.stddev();

		this.setColumnExpression(new SimpleNumericExpression<?,?,?>[]{
			averageExpression,
			maxExpr,
			minExpr,
			sumExpr,
			countExpr,
			stdDevExpression
		});
		this.originalExpression = numberExpressionToGenerateStatsFrom;

	}

	/**
	 * Creates a column expression with a statistics result from the expression
	 * provided.
	 *
	 * <p>
	 * Used in {@link DBReport}, and some {@link DBRow}, sub-classes to derive
	 * data from the database prior to retrieval.
	 *
	 * @param numberExpressionToGenerateStatsFrom numberExpression
	 */
	public DBNumberStatistics(IntegerExpression numberExpressionToGenerateStatsFrom) {
		this(numberExpressionToGenerateStatsFrom.numberResult());
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
	 * Returns the sum of all the numbers in this group.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the sum of all values in this grouping
	 */
	public Number sum() {
		return this.sumNumber;
	}

	/**
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the minimum (smallest) value in this grouping
	 */
	public Number min() {
		return this.minNumber;
	}

	/**
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the maximum (largest) value in this grouping
	 */
	public Number max() {
		return this.maxNumber;
	}

	/**
	 * The middle term in the grouping.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the uniqueRanking value in this grouping
	 */
	public Number median() {
		return this.medianNumber;
	}

	/**
	 * The average value of the grouping.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the average value of the grouping
	 */
	public Number average() {
		return this.averageNumber;
	}

	/**
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the middle number between the uniqueRanking and the smallest value.
	 */
	public Number firstQuartile() {
		return this.firstQuartileNumber;
	}

	/**
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the middle number between the uniqueRanking and the largest value.
	 */
	public Number thirdQuartile() {
		return this.thirdQuartileNumber;
	}

	/**
	 * The middle term in the grouping.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the uniqueRanking value in this grouping
	 */
	public Number secondQuartile() {
		return this.medianNumber;
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
	public DBNumberStatistics copy() {
		DBNumberStatistics copy = (DBNumberStatistics) super.copy();
		copy.averageNumber = this.averageNumber;
		copy.stdDev = this.stdDev;
		copy.countOfRows = this.countOfRows;
		copy.sumNumber = this.sumNumber;
		copy.firstQuartileNumber = this.firstQuartileNumber;
		copy.maxNumber = this.maxNumber;
		copy.medianNumber = this.medianNumber;
		copy.minNumber = this.minNumber;
		copy.thirdQuartileNumber = this.thirdQuartileNumber;
		copy.originalExpression = this.originalExpression;
		return copy;
	}

	@Override
	protected Number getFromResultSet(DBDefinition database, ResultSet resultSet, String fullColumnName) throws SQLException {
		try {
			return resultSet.getBigDecimal(fullColumnName);
		} catch (SQLException ex) {
			try {
				return resultSet.getDouble(fullColumnName);
			} catch (SQLException ex2) {
				try {
					return resultSet.getLong(fullColumnName);
				} catch (SQLException ex3) {
					return null;
				}
			}
		}
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
	public DBNumber getQueryableDatatypeForExpressionValue() {
		return new DBNumberStatistics();
	}

	@Override
	public void setFromResultSet(DBDefinition database, ResultSet resultSet, String resultSetColumnName) throws SQLException {
		removeConstraints();
		if (resultSet == null || resultSetColumnName == null) {
			this.setToNull();
		} else {
			Number dbValue;
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
					final String averageColumnAlias = propertyWrapperDefinition.allColumnAspects.get(0).columnAlias;
					final String maxColumnAlias = propertyWrapperDefinition.allColumnAspects.get(1).columnAlias;
					final String minColumnAlias = propertyWrapperDefinition.allColumnAspects.get(2).columnAlias;
					final String sumColumnAlias = propertyWrapperDefinition.allColumnAspects.get(3).columnAlias;
					final String countColumnAlias = propertyWrapperDefinition.allColumnAspects.get(4).columnAlias;
					final String stdDevColumnAlias = propertyWrapperDefinition.allColumnAspects.get(5).columnAlias;
					averageNumber = getFromResultSet(database, resultSet, averageColumnAlias);
					stdDev = getFromResultSet(database, resultSet, stdDevColumnAlias);
					maxNumber = getFromResultSet(database, resultSet, maxColumnAlias);
					minNumber = getFromResultSet(database, resultSet, minColumnAlias);
					sumNumber = getFromResultSet(database, resultSet, sumColumnAlias);
					countOfRows = getFromResultSet(database, resultSet, countColumnAlias);
				} else {
					averageNumber = getFromResultSet(database, resultSet, resultSetColumnName, 0);
					maxNumber = getFromResultSet(database, resultSet, resultSetColumnName, 1);
					minNumber = getFromResultSet(database, resultSet, resultSetColumnName, 2);
					sumNumber = getFromResultSet(database, resultSet, resultSetColumnName, 3);
					countOfRows = getFromResultSet(database, resultSet, resultSetColumnName, 4);
					stdDev = getFromResultSet(database, resultSet, resultSetColumnName, 5);
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
		return (averageNumber == null ? "" : "count=" + countOfRows + "sum=" + sumNumber + "ave=" + averageNumber + "stdDev=" + stdDev + ":max=" + maxNumber + ":min=" + minNumber);
	}

	public Number standardDeviation() {
		return this.stdDev;
	}

}
