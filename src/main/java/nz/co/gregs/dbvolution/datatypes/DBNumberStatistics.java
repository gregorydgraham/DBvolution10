/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.datatypes;

import java.sql.ResultSet;
import java.sql.SQLException;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapperDefinition;

/**
 *
 * @author gregorygraham
 */
public class DBNumberStatistics extends DBNumber {

	private static final long serialVersionUID = 1;

	protected transient NumberExpression originalExpression;
	protected final DBNumber numberProxy = new DBNumber();
	protected Number minNumber;
	protected Number maxNumber;
	protected Number medianNumber;
	protected Number averageNumber;
	protected Number firstQuartileNumber;
	protected Number thirdQuartileNumber;
	protected Number countOfRows;
	private transient NumberExpression averageExpression;
	private transient NumberExpression maxExpr;
	private transient NumberExpression minExpr;
	private transient NumberExpression sumExpr;
	private transient NumberExpression countExpr;
	private Number sumNumber;

	public DBNumberStatistics() {
	}

	public DBNumberStatistics(NumberExpression numberExpressionToGenerateStatsFrom) {
		super();
		averageExpression = numberExpressionToGenerateStatsFrom.sum().dividedBy(NumberExpression.countAll());
		maxExpr=	numberExpressionToGenerateStatsFrom.max();
		minExpr=	numberExpressionToGenerateStatsFrom.min();
		sumExpr=	numberExpressionToGenerateStatsFrom.sum();
		countExpr= NumberExpression.countAll();

		this.setColumnExpression(new NumberExpression[]{
			averageExpression,
			maxExpr,
			minExpr,
			sumExpr,
			countExpr
		});
		this.originalExpression = numberExpressionToGenerateStatsFrom;

	}

	public Number count() {
		return this.countOfRows;
	}

	public Number sum() {
		return this.sumNumber;
	}

	public Number min() {
		return this.minNumber;
	}

	public Number max() {
		return this.maxNumber;
	}

	public Number median() {
		return this.medianNumber;
	}

	public Number average() {
		return this.averageNumber;
	}

	public Number firstQuartile() {
		return this.firstQuartileNumber;
	}

	public Number thirdQuartile() {
		return this.thirdQuartileNumber;
	}

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
	protected Number getFromResultSet(DBDatabase database, ResultSet resultSet, String fullColumnName) throws SQLException {
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

	@Override
	public void setFromResultSet(DBDatabase database, ResultSet resultSet, String resultSetColumnName) throws SQLException {
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
				if (propertyWrapperDefinition!=null && propertyWrapperDefinition.allColumnAspects != null) {
					final String averageColumnAlias = propertyWrapperDefinition.allColumnAspects.get(0).columnAlias;
					String maxColumnAlias = propertyWrapperDefinition.allColumnAspects.get(1).columnAlias;
					String minColumnAlias = propertyWrapperDefinition.allColumnAspects.get(2).columnAlias;
					String sumColumnAlias = propertyWrapperDefinition.allColumnAspects.get(3).columnAlias;
					String countColumnAlias = propertyWrapperDefinition.allColumnAspects.get(4).columnAlias;
					averageNumber = getFromResultSet(database, resultSet, averageColumnAlias);
					maxNumber = getFromResultSet(database, resultSet, maxColumnAlias);
					minNumber = getFromResultSet(database, resultSet, minColumnAlias);
					sumNumber = getFromResultSet(database, resultSet, sumColumnAlias);
					countOfRows = getFromResultSet(database, resultSet, countColumnAlias);
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
		return (averageNumber == null ? "" : "count="+countOfRows+"sum="+sumNumber+"ave="+averageNumber+":max="+maxNumber+":min="+minNumber);
	}
	

}
