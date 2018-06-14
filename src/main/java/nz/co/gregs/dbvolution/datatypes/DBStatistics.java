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
import nz.co.gregs.dbvolution.expressions.RangeExpression;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapperDefinition;
import nz.co.gregs.dbvolution.results.EqualResult;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 * @param <B>
 * @param <R>
 * @param <D>
 * @param <X>
 */
public class DBStatistics<B, R extends EqualResult<B>, D extends QueryableDatatype<B>, X extends EqualExpression<B, R, D>> extends DBString {

	private static final long serialVersionUID = 1;

	private final X originalExpression;
	private final DBString numberProxy = new DBString();
	private Number countOfRows;
	private B modeSimple;
	private B modeStrict;
	private B median;
	private B firstQuartileValue;
	private B thirdQuartileValue;
	private IntegerExpression countExpr;
	private EqualExpression.ModeSimpleExpression<B, R, D, X> modeSimpleExpression;
	private EqualExpression.ModeStrictExpression<B, R, D, X> modeStrictExpression;
	private RangeExpression.UniqueRankingExpression<B, R, D, X> uniqueRankingExpression;
	private RangeExpression.MedianExpression<B, R, D, X> medianExpression;
	private X firstQuartileExpression;
	private X thirdQuartileExpression;

	/**
	 * The default constructor for DBStatistics.
	 *
	 * <p>
	 * Creates an unset undefined DBNumber object.</p>
	 *
	 * <p>
	 * Use {@link #DBStatistics(nz.co.gregs.dbvolution.expressions.EqualExpression)
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
	 * @param expressionToGenerateStatsFrom the expression or column to be used
	 * for statistics
	 */
	public DBStatistics(X expressionToGenerateStatsFrom) {
		super(expressionToGenerateStatsFrom.stringResult());
		this.originalExpression = expressionToGenerateStatsFrom;
		countExpr = originalExpression.count();
		modeSimpleExpression = new EqualExpression.ModeSimpleExpression<B, R, D, X>(originalExpression);
		modeStrictExpression = new EqualExpression.ModeStrictExpression<B, R, D, X>(originalExpression);
//		medianExpression = new EqualExpression.UniqueRankingExpression<B, R, D, X>(originalExpression);

		this.setColumnExpression(new AnyExpression<?, ?, ?>[]{
			countExpr,
			 modeSimpleExpression,
			 modeStrictExpression
			 //,medianExpression
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
	public B modeSimple() {
		return this.modeSimple;
	}

	/**
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the maximum (largest) value in this grouping
	 */
	public B modeStrict() {
		return this.modeStrict;
	}

	/**
	 * The median value, that is the value that occurs halfway through the
	 * distribution.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the ranking
	 */
	public B median() {
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
	public B firstQuartile() {
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
	public B thirdQuartile() {
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
	@SuppressWarnings("unchecked")
	public DBStatistics<B, R, D, X> copy() {
		DBStatistics<B, R, D, X> copy = (DBStatistics<B, R, D, X>) super.copy();
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
	public DBStatistics<B, R, D, X> getQueryableDatatypeForExpressionValue() {
		return new DBStatistics<>();
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
					final String modeSimpleAlias = propertyWrapperDefinition.allColumnAspects.get(1).columnAlias;
					final String modeStrictAlias = propertyWrapperDefinition.allColumnAspects.get(2).columnAlias;
//					final String medianAlias = propertyWrapperDefinition.allColumnAspects.get(3).columnAlias;
					countOfRows = new DBInteger().getFromResultSet(database, resultSet, countColumnAlias);
					modeSimple = modeSimpleExpression.asExpressionColumn().getFromResultSet(database, resultSet, modeSimpleAlias);
					modeStrict = modeStrictExpression.asExpressionColumn().getFromResultSet(database, resultSet, modeStrictAlias);
//					median = medianExpression.asExpressionColumn().getFromResultSet(database, resultSet, medianAlias);
				} else {
					countOfRows = getFromResultSet(database, resultSet, resultSetColumnName, 0);
//					modeSimple = modeSimpleExpression.asExpressionColumn().getFromResultSet(database, resultSet, resultSetColumnName, 1);
//					modeStrict = getFromResultSet(database, resultSet, resultSetColumnName, 1);
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
		return ("count=" + countOfRows);
	}

}
