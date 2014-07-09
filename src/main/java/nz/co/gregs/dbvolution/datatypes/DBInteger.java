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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.expressions.NumberResult;
import nz.co.gregs.dbvolution.operators.DBPermittedRangeExclusiveOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedRangeInclusiveOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedRangeOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedValuesOperator;

/**
 *
 * @author Gregory Graham
 */
public class DBInteger extends QueryableDatatype {

    private static final long serialVersionUID = 1L;

    public DBInteger(Object someNumber) {
        this(Long.parseLong(someNumber.toString()));
    }

    public DBInteger(int anInt) {
        this(Integer.valueOf(anInt));
    }

    public DBInteger(Integer anInt) {
        super(anInt);
    }

    public DBInteger(long aLong) {
        this(Long.valueOf(aLong));
    }

    public DBInteger(Long aLong) {
        super(aLong);
    }

    public DBInteger(NumberResult aLong) {
        super(aLong);
    }

    @Deprecated
    public DBInteger(double aDouble) {
        this(new Double(aDouble));
    }

    @Deprecated
    public DBInteger(Double aDouble) {
        this(aDouble == null ? null : aDouble.longValue());
    }

    public DBInteger() {
        super();
    }

    @Override
    public String getSQLDatatype() {
        return "INTEGER";
    }

    @Override
    public void setFromResultSet(DBDatabase database, ResultSet resultSet, String fullColumnName) {
        blankQuery();
        if (resultSet == null || fullColumnName == null) {
            this.setToNull();
        } else {
            Long dbValue;
            try {
                dbValue = resultSet.getLong(fullColumnName);
                if (resultSet.wasNull()) {
                    dbValue = null;
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

    /**
     * Returns a Long of the database value or NULL if the database value is
     * null
     *
     * @return the long value or null
     */
    @Override
    public Long getValue() {
        if (this.literalValue instanceof Long) {
            return (Long) this.literalValue;
        } else if (this.literalValue == null) {
            return null;
        } else {
            return Long.parseLong(this.literalValue.toString());
        }
    }

    /**
     * Returns an Integer of the database value or NULL if the database value is
     * null
     *
     * @return the integer value or null
     */
    @Override
    @SuppressWarnings("deprecation")
    public Integer intValue() {
        Long value = getValue();
        return value == null ? null : value.intValue();
    }

    /**
     * Returns a Long of the database value or NULL if the database value is
     * null
     *
     * @return the long value or null
     */
    @Override
    @SuppressWarnings("deprecation")
    public Long longValue() {
        return getValue();
    }

    /**
     *
     * reduces the rows to only the object, Set, List, Array, or vararg of
     * objects
     *
     * @param permitted
     */
    public void permittedValues(Long... permitted) {
        this.setOperator(new DBPermittedValuesOperator((Object[]) permitted));
    }

    /**
     *
     * reduces the rows to only the object, Set, List, Array, or vararg of
     * objects
     *
     * @param permitted
     */
    public void permittedValues(NumberResult... permitted) {
        this.setOperator(new DBPermittedValuesOperator((Object[]) permitted));
    }

    /**
     *
     * reduces the rows to only the object, Set, List, Array, or vararg of
     * objects
     *
     * @param permitted
     */
    public void permittedValues(Number... permitted) {
        List<Long> ints = new ArrayList<Long>();
        for (Number dbint : permitted) {
            ints.add(dbint.longValue());
        }
        final Long[] longArray = ints.toArray(new Long[]{});
        this.setOperator(new DBPermittedValuesOperator((Object[]) longArray));
    }

    /**
     *
     * reduces the rows to only the object, Set, List, Array, or vararg of
     * objects
     *
     * @param permitted
     */
    public void permittedValues(DBInteger... permitted) {
        List<Long> ints = new ArrayList<Long>();
        for (DBInteger dbint : permitted) {
            ints.add(dbint.getValue());
        }
        final Long[] longArray = ints.toArray(new Long[]{});
        this.setOperator(new DBPermittedValuesOperator((Object[]) longArray));
    }

    /**
     *
     * reduces the rows to only the object, Set, List, Array, or vararg of
     * objects
     *
     * @param permitted
     */
    public void permittedValues(DBNumber... permitted) {
        List<Long> ints = new ArrayList<Long>();
        for (DBNumber dbint : permitted) {
            ints.add(dbint.getValue().longValue());
        }
        final Long[] longArray = ints.toArray(new Long[]{});
        this.setOperator(new DBPermittedValuesOperator((Object[]) longArray));
    }

    /**
     *
     * reduces the rows to only the object, Set, List, Array, or vararg of
     * objects
     *
     * @param permitted
     */
    public void permittedValues(Collection<Long> permitted) {
        this.setOperator(new DBPermittedValuesOperator(permitted));
    }

    /**
     *
     * reduces the rows to only the object, Set, List, Array, or vararg of
     * objects
     *
     * @param permitted
     */
    public void permittedValuesInteger(Collection<Integer> permitted) {
        this.setOperator(new DBPermittedValuesOperator(permitted));
    }

    /**
     *
     * reduces the rows to only the object, Set, List, Array, or vararg of
     * objects
     *
     * @param permitted
     */
    public void permittedValues(Integer... permitted) {
        this.setOperator(new DBPermittedValuesOperator((Object[]) permitted));
    }

    /**
     *
     * excludes the object, Set, List, Array, or vararg of objects
     *
     *
     * @param excluded
     */
    public void excludedValues(Long... excluded) {
        this.setOperator(new DBPermittedValuesOperator((Object[]) excluded));
        negateOperator();
    }

    /**
     *
     * excludes the object, Set, List, Array, or vararg of objects
     *
     *
     * @param excluded
     */
    public void excludedValues(DBInteger... excluded) {
        this.setOperator(new DBPermittedValuesOperator((Object[]) excluded));
        negateOperator();
    }

    /**
     *
     * excludes the object, Set, List, Array, or vararg of objects
     *
     *
     * @param excluded
     */
    public void excludedValues(Integer... excluded) {
        this.setOperator(new DBPermittedValuesOperator((Object[]) excluded));
        negateOperator();
    }

    /**
     *
     * excludes the object, Set, List, Array, or vararg of objects
     *
     *
     * @param excluded
     */
    public void excludedValuesLong(List<Long> excluded) {
        this.setOperator(new DBPermittedValuesOperator(excluded));
        negateOperator();
    }

    /**
     *
     * excludes the object, Set, List, Array, or vararg of objects
     *
     *
     * @param excluded
     */
    public void excludedValuesInteger(List<Integer> excluded) {
        this.setOperator(new DBPermittedValuesOperator(excluded));
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
    public void permittedRange(Long lowerBound, Long upperBound) {
        setOperator(new DBPermittedRangeOperator(lowerBound, upperBound));
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
    public void permittedRange(Integer lowerBound, Integer upperBound) {
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
    public void permittedRangeInclusive(Long lowerBound, Long upperBound) {
        setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
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
    public void permittedRangeInclusive(Integer lowerBound, Integer upperBound) {
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
    public void permittedRangeExclusive(Long lowerBound, Long upperBound) {
        setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
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
    public void permittedRangeExclusive(Integer lowerBound, Integer upperBound) {
        setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
    }

    public void excludedRange(Long lowerBound, Long upperBound) {
        setOperator(new DBPermittedRangeOperator(lowerBound, upperBound));
        negateOperator();
    }

    public void excludedRange(Integer lowerBound, Integer upperBound) {
        setOperator(new DBPermittedRangeOperator(lowerBound, upperBound));
        negateOperator();
    }

    public void excludedRangeInclusive(Long lowerBound, Long upperBound) {
        setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
        negateOperator();
    }

    public void excludedRangeInclusive(Integer lowerBound, Integer upperBound) {
        setOperator(new DBPermittedRangeInclusiveOperator(lowerBound, upperBound));
        negateOperator();
    }

    public void excludedRangeExclusive(Long lowerBound, Long upperBound) {
        setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
        negateOperator();
    }

    public void excludedRangeExclusive(Integer lowerBound, Integer upperBound) {
        setOperator(new DBPermittedRangeExclusiveOperator(lowerBound, upperBound));
        negateOperator();
    }

    @Override
    public void setValue(Object newLiteralValue) {
        if (newLiteralValue == null) {
            super.setLiteralValue(null);
//        } else if (newLiteralValue instanceof Long) {
//            super.setLiteralValue(newLiteralValue);
//        } else if (newLiteralValue instanceof Integer) {
//            super.setLiteralValue(newLiteralValue);
//        } else if (newLiteralValue instanceof Number) {
//            super.setLiteralValue(newLiteralValue);
//        } else if (newLiteralValue instanceof DBInteger) {
//            final DBInteger dbInteger = (DBInteger) newLiteralValue;
//            setValue(dbInteger.getValue());
//        } else if (newLiteralValue instanceof DBNumber) {
//            final DBNumber dbNumber = (DBNumber) newLiteralValue;
//            setValue(dbNumber.getValue());
        } else if (newLiteralValue.toString().isEmpty()) {
            super.setLiteralValue(null);
        } else {
            try {
                Long literalLong = Long.parseLong(newLiteralValue.toString());
                setLiteralValue(literalLong);
            } catch (NumberFormatException noFormat) {
                setLiteralValue(null);
            }
        }
    }

    public void setValue(DBNumber newLiteralValue) {
        setValue((newLiteralValue).getValue());
    }

    public void setValue(DBInteger newLiteralValue) {
        setValue((newLiteralValue).getValue());
    }

    public void setValue(Number newLiteralValue) {
        if (newLiteralValue == null) {
            super.setLiteralValue(null);
        } else {
            super.setLiteralValue(newLiteralValue);
        }
    }

    public void setValue(Long newLiteralValue) {
        if (newLiteralValue == null) {
            super.setLiteralValue(null);
        } else {
            super.setLiteralValue(newLiteralValue);
        }
    }

    public void setValue(Integer newLiteralValue) {
        if (newLiteralValue == null) {
            super.setLiteralValue(null);
        } else {
            super.setLiteralValue(newLiteralValue);
        }
    }

    /**
     *
     * @param db
     * @return the underlying number formatted for a SQL statement
     */
    @Override
    public String formatValueForSQLStatement(DBDatabase db) {
        DBDefinition defn = db.getDefinition();
        if (isNull()) {
            return defn.getNull();
        }
        return defn.beginNumberValue() + literalValue.toString() + defn.endNumberValue();
    }

    @Override
    public boolean isAggregator() {
        return false;
    }

    @Override
    public void blankQuery() {
        super.blankQuery();
    }

}
