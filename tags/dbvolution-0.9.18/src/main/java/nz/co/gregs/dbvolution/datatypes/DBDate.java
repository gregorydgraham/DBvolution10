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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.expressions.DateExpression;
import nz.co.gregs.dbvolution.operators.DBLikeCaseInsensitiveOperator;
import nz.co.gregs.dbvolution.expressions.DateResult;
import nz.co.gregs.dbvolution.operators.DBPermittedRangeExclusiveOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedRangeInclusiveOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedRangeOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedValuesOperator;

/**
 *
 * @author Gregory Graham
 */
public class DBDate extends QueryableDatatype implements DateResult {

    private static final long serialVersionUID = 1L;

    public DBDate() {
        super();
    }

    public DBDate(Date date) {
        super(date);
    }

    public DBDate(DateResult dateExpression) {
        super(dateExpression);
    }

    DBDate(Timestamp timestamp) {
        super(timestamp);
        if (timestamp == null) {
            this.isDBNull = true;
        } else {
            Date date = new Date();
            date.setTime(timestamp.getTime());
            literalValue = date;
        }
    }

    @SuppressWarnings("deprecation")
    DBDate(String str) {
        final long dateLong = Date.parse(str);
        Date dateValue = new Date();
        dateValue.setTime(dateLong);
        literalValue = dateValue;
    }

    @Override
    public String getWhereClause(DBDatabase db, String columnName) {
        if (this.getOperator() instanceof DBLikeCaseInsensitiveOperator) {
            throw new RuntimeException("DATE COLUMNS CAN'T USE \"LIKE\": " + columnName);
        } else {
            return super.getWhereClause(db, columnName);
        }
    }

    public Date dateValue() {
        if (literalValue instanceof Date) {
            return (Date) literalValue;
        } else {
            return null;
        }
    }

    @Override
    public void setValue(Object newLiteralValue) {
        if (newLiteralValue instanceof Date) {
            setValue((Date) newLiteralValue);
        } else if (newLiteralValue instanceof DBDate) {
            setValue(((QueryableDatatype) newLiteralValue).literalValue);
        } else {
            throw new ClassCastException(this.getClass().getSimpleName() + ".setValue() Called With A Non-Date: Use only Dates with this class");
        }
    }

    public void setValue(Date date) {
        super.setLiteralValue(date);
    }

    @SuppressWarnings("deprecation")
    public void setValue(String dateStr) {
        final long dateLong = Date.parse(dateStr);
        Date date = new Date();
        date.setTime(dateLong);
        setValue(date);
    }

    @Override
    public String getSQLDatatype() {
        return "TIMESTAMP";
    }

    @Override
    public String toString() {
        if (this.isDBNull || dateValue() == null) {
            return "";
        }
        return dateValue().toString();
    }

    @Override
    public String formatValueForSQLStatement(DBDatabase db) {
        return db.getDefinition().getDateFormattedForQuery(dateValue());
    }

    @Override
    public void setFromResultSet(ResultSet resultSet, String fullColumnName) {
        blankQuery();
        if (resultSet == null || fullColumnName == null) {
            this.setToNull();
        } else {
            java.sql.Date dbValue;
            try {
                java.sql.Date dateValue = resultSet.getDate(fullColumnName);
                if (resultSet.wasNull()) {
                    dbValue = null;
                } else {
                    // Some drivers interpret getDate as meaning return only the date without the time
                    // so we should check both the date and the timestamp find the latest time.
                    final long timestamp = resultSet.getTimestamp(fullColumnName).getTime();
                    java.sql.Date timestampValue = new java.sql.Date(timestamp);
                    if (timestampValue.after(dateValue)) {
                        dbValue = timestampValue;
                    } else {
                        dbValue = dateValue;
                    }
                }

            } catch (SQLException ex) {
                dbValue = null;
            }
            if (dbValue == null) {
                this.setToNull();
            } else {
                this.setValue(dbValue);
            }
        }
        setUnchanged();
        setDefined(true);
    }

    @Override
    public DBDate copy() {
        return (DBDate) super.copy(); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Date getValue() {
        return dateValue();
    }

    @Override
    public DBDate getQueryableDatatypeForExpressionValue() {
        return new DBDate();
    }

    @Override
    public boolean isAggregator() {
        return false;
    }

    @Override
    public Set<DBRow> getTablesInvolved() {
        return new HashSet<DBRow>();
    }

    /**
     *
     * reduces the rows to only the object, Set, List, Array, or vararg of
     * objects
     *
     * @param permitted
     */
    public void permittedValues(Date... permitted) {
        this.setOperator(new DBPermittedValuesOperator((Object[])permitted));
    }

    /**
     *
     * excludes the object, Set, List, Array, or vararg of objects
     *
     *
     * @param excluded
     */
    public void excludedValues(Date... excluded) {
        this.setOperator(new DBPermittedValuesOperator((Object[])excluded));
        negateOperator();
    }

    /**
     * Performs searches based on a range.
     *
     * if both ends of the range are specified the lower-bound will be included
     * in the search and the upper-bound excluded. I.e permittedRange(1,3) will
     * return 1 and 2.
     *
     * <p>
     * if the upper-bound is null the range will be open ended and inclusive.
     * <br>
     * I.e permittedRange(1,null) will return 1,2,3,4,5, etc.
     *
     * <p>
     * if the upper-bound is null the range will be open ended and exclusive.
     * <br>
     * I.e permittedRange(null, 5) will return 4,3,2,1, etc.
     *
     * @param lowerBound
     * @param upperBound
     */
    public void permittedRange(Date lowerBound, Date upperBound) {
        setOperator(new DBPermittedRangeOperator(lowerBound, upperBound));
    }

    /**
     * Performs searches based on a range.
     *
     * if both ends of the range are specified both the lower- and upper-bound
     * will be included in the search. I.e permittedRangeInclusive(1,3) will
     * return 1, 2, and 3.
     *
     * <p>
     * if the upper-bound is null the range will be open ended and inclusive.
     * <br>
     * I.e permittedRangeInclusive(1,null) will return 1,2,3,4,5, etc.
     *
     * <p>
     * if the upper-bound is null the range will be open ended and inclusive.
     * <br>
     * I.e permittedRangeInclusive(null, 5) will return 5,4,3,2,1, etc.
     *
     * @param lowerBound
     * @param upperBound
     */
    public void permittedRangeInclusive(Date lowerBound, Date upperBound) {
        setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
    }

    /**
     * Performs searches based on a range.
     *
     * if both ends of the range are specified both the lower- and upper-bound
     * will be excluded in the search. I.e permittedRangeExclusive(1,3) will
     * return 2.
     *
     * <p>
     * if the upper-bound is null the range will be open ended and exclusive.
     * <br>
     * I.e permittedRangeExclusive(1,null) will return 2,3,4,5, etc.
     *
     * <p>
     * if the upper-bound is null the range will be open ended and exclusive.
     * <br>
     * I.e permittedRangeExclusive(null, 5) will return 4,3,2,1, etc.
     *
     * @param lowerBound
     * @param upperBound
     */
    public void permittedRangeExclusive(Date lowerBound, Date upperBound) {
        setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
    }

    public void excludedRange(Date lowerBound, Date upperBound) {
        setOperator(new DBPermittedRangeOperator(lowerBound, upperBound));
        negateOperator();
    }

    public void excludedRangeInclusive(Date lowerBound, Date upperBound) {
        setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
        negateOperator();
    }

    public void excludedRangeExclusive(Date lowerBound, Date upperBound) {
        setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
        negateOperator();
    }

    /**
     *
     * reduces the rows to only the object, Set, List, Array, or vararg of
     * objects
     *
     * @param permitted
     */
    public void permittedValues(DateExpression... permitted) {
        this.setOperator(new DBPermittedValuesOperator((Object[])permitted));
    }

    /**
     *
     * excludes the object, Set, List, Array, or vararg of objects
     *
     *
     * @param excluded
     */
    public void excludedValues(DateExpression... excluded) {
        this.setOperator(new DBPermittedValuesOperator((Object[])excluded));
        negateOperator();
    }

    /**
     * Performs searches based on a range.
     *
     * if both ends of the range are specified the lower-bound will be included
     * in the search and the upper-bound excluded. I.e permittedRange(1,3) will
     * return 1 and 2.
     *
     * <p>
     * if the upper-bound is null the range will be open ended and inclusive.
     * <br>
     * I.e permittedRange(1,null) will return 1,2,3,4,5, etc.
     *
     * <p>
     * if the upper-bound is null the range will be open ended and exclusive.
     * <br>
     * I.e permittedRange(null, 5) will return 4,3,2,1, etc.
     *
     * @param lowerBound
     * @param upperBound
     */
    public void permittedRange(DateExpression lowerBound, DateExpression upperBound) {
        setOperator(new DBPermittedRangeOperator(lowerBound, upperBound));
    }

    /**
     * Performs searches based on a range.
     *
     * if both ends of the range are specified both the lower- and upper-bound
     * will be included in the search. I.e permittedRangeInclusive(1,3) will
     * return 1, 2, and 3.
     *
     * <p>
     * if the upper-bound is null the range will be open ended and inclusive.
     * <br>
     * I.e permittedRangeInclusive(1,null) will return 1,2,3,4,5, etc.
     *
     * <p>
     * if the upper-bound is null the range will be open ended and inclusive.
     * <br>
     * I.e permittedRangeInclusive(null, 5) will return 5,4,3,2,1, etc.
     *
     * @param lowerBound
     * @param upperBound
     */
    public void permittedRangeInclusive(DateExpression lowerBound, DateExpression upperBound) {
        setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
    }

    /**
     * Performs searches based on a range.
     *
     * if both ends of the range are specified both the lower- and upper-bound
     * will be excluded in the search. I.e permittedRangeExclusive(1,3) will
     * return 2.
     *
     * <p>
     * if the upper-bound is null the range will be open ended and exclusive.
     * <br>
     * I.e permittedRangeExclusive(1,null) will return 2,3,4,5, etc.
     *
     * <p>
     * if the upper-bound is null the range will be open ended and exclusive.
     * <br>
     * I.e permittedRangeExclusive(null, 5) will return 4,3,2,1, etc.
     *
     * @param lowerBound
     * @param upperBound
     */
    public void permittedRangeExclusive(DateExpression lowerBound, DateExpression upperBound) {
        setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
    }

    public void excludedRange(DateExpression lowerBound, DateExpression upperBound) {
        setOperator(new DBPermittedRangeOperator(lowerBound, upperBound));
        negateOperator();
    }

    public void excludedRangeInclusive(DateExpression lowerBound, DateExpression upperBound) {
        setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
        negateOperator();
    }

    public void excludedRangeExclusive(DateExpression lowerBound, DateExpression upperBound) {
        setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
        negateOperator();
    }
}
