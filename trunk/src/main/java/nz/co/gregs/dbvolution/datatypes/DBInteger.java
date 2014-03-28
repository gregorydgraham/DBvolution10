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
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.expressions.NumberResult;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.operators.DBPermittedPatternOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedRangeExclusiveOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedRangeInclusiveOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedRangeOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedValuesIgnoreCaseOperator;
import nz.co.gregs.dbvolution.operators.DBPermittedValuesOperator;

/**
 *
 * @author Gregory Graham
 */
public class DBInteger extends DBNumber {

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
    public void setFromResultSet(ResultSet resultSet, String fullColumnName) {
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

    @Override
    public Long getValue() {
        return longValue();
    }

    /**
     *
     * reduces the rows to only the object, Set, List, Array, or vararg of
     * objects
     *
     * @param permitted
     */
    public void permittedValues(Long... permitted) {
        this.setOperator(new DBPermittedValuesOperator((Object[])permitted));
    }

    /**
     *
     * reduces the rows to only the object, Set, List, Array, or vararg of
     * objects
     *
     * @param permitted
     */
    public void permittedValues(Integer... permitted) {
        this.setOperator(new DBPermittedValuesOperator((Object[])permitted));
    }

    /**
     *
     * excludes the object, Set, List, Array, or vararg of objects
     *
     *
     * @param excluded
     */
    public void excludedValues(Long... excluded) {
        this.setOperator(new DBPermittedValuesOperator((Object[])excluded));
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
}
