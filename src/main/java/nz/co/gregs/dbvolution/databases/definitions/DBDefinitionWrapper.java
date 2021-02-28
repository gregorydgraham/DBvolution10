/*
 * Copyright 2020 Gregory Graham.
 *
 * Commercial licenses are available, please contact info@gregs.co.nz for details.
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
package nz.co.gregs.dbvolution.databases.definitions;

import com.vividsolutions.jts.geom.*;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import nz.co.gregs.dbvolution.expressions.SortProvider;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.generation.DBTableField;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import nz.co.gregs.dbvolution.internal.query.LargeObjectHandlerType;
import nz.co.gregs.dbvolution.internal.query.QueryOptions;
import nz.co.gregs.dbvolution.internal.query.QueryState;
import nz.co.gregs.dbvolution.query.RowDefinition;
import org.joda.time.Period;

/**
 *
 * @author gregorygraham
 */
class DBDefinitionWrapper extends DBDefinition {

	private static final long serialVersionUID = 1L;

	private final DBDefinition base;

	private DBDefinitionWrapper(DBDefinition defn) {
		base = defn;
	}

	public static DBDefinitionWrapper wrap(DBDefinition defn) {
		return new DBDefinitionWrapper(defn);
	}

	@Override
	public int getNumericPrecision() {
		return base.getNumericPrecision();
	}

	@Override
	public int getNumericScale() {
		return base.getNumericScale();
	}

	@Override
	public String getDateFormattedForQuery(Date date) {
		return base.getDateFormattedForQuery(date);
	}

	@Override
	public String getLocalDateTimeFormattedForQuery(LocalDateTime date) {
		return base.getLocalDateTimeFormattedForQuery(date);
	}

	@Override
	public String getInstantFormattedForQuery(Instant instant) {
		return base.getInstantFormattedForQuery(instant);
	}

	@Override
	public String getLocalDatePartsFormattedForQuery(String years, String months, String days, String hours, String minutes, String seconds, String subsecond, String timeZoneSign, String timeZoneHourOffset, String timeZoneMinuteOffSet) {
		return base.getLocalDatePartsFormattedForQuery(years, months, days, hours, minutes, seconds, subsecond, timeZoneSign, timeZoneHourOffset, timeZoneMinuteOffSet);
	}

	@Override
	public String getInstantPartsFormattedForQuery(String years, String months, String days, String hours, String minutes, String seconds, String subsecond, String timeZoneSign, String timeZoneHourOffset, String timeZoneMinuteOffSet) {
		return base.getInstantPartsFormattedForQuery(years, months, days, hours, minutes, seconds, subsecond, timeZoneSign, timeZoneHourOffset, timeZoneMinuteOffSet);
	}

	@Override
	public String getDatePartsFormattedForQuery(String years, String months, String days, String hours, String minutes, String seconds, String subsecond, String timeZoneSign, String timeZoneHourOffset, String timeZoneMinuteOffSet) {
		return base.getDatePartsFormattedForQuery(years, months, days, hours, minutes, seconds, subsecond, timeZoneSign, timeZoneHourOffset, timeZoneMinuteOffSet);
	}

	@Override
	public String getUTCDateFormattedForQuery(Date date) {
		return base.getUTCDateFormattedForQuery(date);
	}

	@Override
	public String formatColumnName(String columnName) {
		return base.formatColumnName(columnName);
	}

	@Override
	public String beginStringValue() {
		return base.beginStringValue();
	}

	@Override
	public String endStringValue() {
		return base.endStringValue();
	}

	@Override
	public String beginNumberValue() {
		return base.beginNumberValue();
	}

	@Override
	public String endNumberValue() {
		return base.endNumberValue();
	}

	@Override
	public String formatTableAndColumnName(DBRow table, String columnName) {
		return base.formatTableAndColumnName(table, columnName);
	}

	@Override
	public String formatTableAliasAndColumnName(RowDefinition table, String columnName) {
		return base.formatTableAliasAndColumnName(table, columnName);
	}

	@Override
	public String formatTableAliasAndColumnNameForSelectClause(DBRow table, String columnName) {
		return base.formatTableAliasAndColumnNameForSelectClause(table, columnName);
	}

	@Override
	public String formatTableName(DBRow table) {
		return base.formatTableName(table);
	}

	@Override
	public String formatColumnNameForDBQueryResultSet(RowDefinition table, String columnName) {
		return base.formatColumnNameForDBQueryResultSet(table, columnName);
	}

	@Override
	public String formatForColumnAlias(String actualName) {
		return base.formatForColumnAlias(actualName);
	}

	@Override
	public String getTableAliasForObject(Object anObject) {
		return base.getTableAliasForObject(anObject);
	}

	@Override
	protected String formatNameForDatabase(String sqlObjectName) {
		return base.formatNameForDatabase(sqlObjectName);
	}

	@Override
	public String formatExpressionAlias(Object key) {
		return base.formatExpressionAlias(key);
	}

	@Override
	public String safeString(String toString) {
		return base.safeString(toString);
	}

	@Override
	public String beginWhereClauseLine() {
		return base.beginWhereClauseLine();
	}

	@Override
	public String beginConditionClauseLine(QueryOptions options) {
		return base.beginConditionClauseLine(options);
	}

	@Override
	public String beginJoinClauseLine(QueryOptions options) {
		return base.beginJoinClauseLine(options);
	}

	@Override
	public boolean prefersIndexBasedGroupByClause() {
		return base.prefersIndexBasedGroupByClause();
	}

	@Override
	public String beginAndLine() {
		return base.beginAndLine();
	}

	@Override
	public String beginOrLine() {
		return base.beginOrLine();
	}

	@Override
	public String getDropTableStart() {
		return base.getDropTableStart();
	}

	@Override
	public String getCreateTablePrimaryKeyClauseStart() {
		return base.getCreateTablePrimaryKeyClauseStart();
	}

	@Override
	public String getCreateTablePrimaryKeyClauseMiddle() {
		return base.getCreateTablePrimaryKeyClauseMiddle();
	}

	@Override
	public String getCreateTablePrimaryKeyClauseEnd() {
		return base.getCreateTablePrimaryKeyClauseEnd();
	}

	@Override
	public String getCreateTableStart() {
		return base.getCreateTableStart();
	}

	@Override
	public String getCreateTableColumnsStart() {
		return base.getCreateTableColumnsStart();
	}

	@Override
	public String getCreateTableColumnsSeparator() {
		return base.getCreateTableColumnsSeparator();
	}

	@Override
	public String getCreateTableColumnsNameAndTypeSeparator() {
		return base.getCreateTableColumnsNameAndTypeSeparator();
	}

	@Override
	public Object getCreateTableColumnsEnd() {
		return base.getCreateTableColumnsEnd();
	}

	@Override
	public String toLowerCase(String sql) {
		return base.toLowerCase(sql);
	}

	@Override
	public String beginInsertLine() {
		return base.beginInsertLine();
	}

	@Override
	public String endInsertLine() {
		return base.endInsertLine();
	}

	@Override
	public String beginInsertColumnList() {
		return base.beginInsertColumnList();
	}

	@Override
	public String endInsertColumnList() {
		return base.endInsertColumnList();
	}

	@Override
	public String beginDeleteLine() {
		return base.beginDeleteLine();
	}

	@Override
	public String endDeleteLine() {
		return base.endDeleteLine();
	}

	@Override
	public String getEqualsComparator() {
		return base.getEqualsComparator();
	}

	@Override
	public String getNotEqualsComparator() {
		return base.getNotEqualsComparator();
	}

	@Override
	public String beginWhereClause() {
		return base.beginWhereClause();
	}

	@Override
	public String beginUpdateLine() {
		return base.beginUpdateLine();
	}

	@Override
	public String beginSetClause() {
		return base.beginSetClause();
	}

	@Override
	public String getStartingSetSubClauseSeparator() {
		return base.getStartingSetSubClauseSeparator();
	}

	@Override
	public String getSubsequentSetSubClauseSeparator() {
		return base.getSubsequentSetSubClauseSeparator();
	}

	@Override
	public String getStartingOrderByClauseSeparator() {
		return base.getStartingOrderByClauseSeparator();
	}

	@Override
	public String getSubsequentOrderByClauseSeparator() {
		return base.getSubsequentOrderByClauseSeparator();
	}

	@Override
	public String getWhereClauseBeginningCondition() {
		return base.getWhereClauseBeginningCondition();
	}

	@Override
	public String getWhereClauseBeginningCondition(QueryOptions options) {
		return base.getWhereClauseBeginningCondition(options);
	}

	@Override
	public String getFalseOperation() {
		return base.getFalseOperation();
	}

	@Override
	public String getTrueOperation() {
		return base.getTrueOperation();
	}

	@Override
	public String getNull() {
		return base.getNull();
	}

	@Override
	public String beginSelectStatement() {
		return base.beginSelectStatement();
	}

	@Override
	public String beginFromClause() {
		return base.beginFromClause();
	}

	@Override
	public String getFromDualEquivalent() {
		return base.getFromDualEquivalent();
	}

	@Override
	public String endSQLStatement() {
		return base.endSQLStatement();
	}

	@Override
	public String getStartingSelectSubClauseSeparator() {
		return base.getStartingSelectSubClauseSeparator();
	}

	@Override
	public String getSubsequentSelectSubClauseSeparator() {
		return base.getSubsequentSelectSubClauseSeparator();
	}

	@Override
	public String countStarClause() {
		return base.countStarClause();
	}

	@Override
	public String getLimitRowsSubClauseDuringSelectClause(QueryOptions options) {
		return base.getLimitRowsSubClauseDuringSelectClause(options);
	}

	@Override
	public String beginOrderByClause() {
		return base.beginOrderByClause();
	}

	@Override
	public String endOrderByClause() {
		return base.endOrderByClause();
	}

	@Override
	public String getOrderByDirectionClause(Boolean sortOrder) {
		return base.getOrderByDirectionClause(sortOrder);
	}

	@Override
	public String getOrderByDirectionClause(SortProvider.Ordering sortOrder) {
		return base.getOrderByDirectionClause(sortOrder);
	}

	@Override
	public String getOrderByDescending() {
		return base.getOrderByDescending();
	}

	@Override
	public String getOrderByAscending() {
		return base.getOrderByAscending();
	}

	@Override
	public String beginInnerJoin() {
		return base.beginInnerJoin();
	}

	@Override
	public String beginLeftOuterJoin() {
		return base.beginLeftOuterJoin();
	}

	@Override
	public String beginRightOuterJoin() {
		return base.beginRightOuterJoin();
	}

	@Override
	public String beginFullOuterJoin() {
		return base.beginFullOuterJoin();
	}

	@Override
	public String beginOnClause() {
		return base.beginOnClause();
	}

	@Override
	public String endOnClause() {
		return base.endOnClause();
	}

	@Override
	protected String getDatabaseDataTypeOfQueryableDatatype(QueryableDatatype<?> qdt) {
		return base.getDatabaseDataTypeOfQueryableDatatype(qdt);
	}

	@Override
	public String getLimitRowsSubClauseAfterWhereClause(QueryState state, QueryOptions options) {
		return base.getLimitRowsSubClauseAfterWhereClause(state, options);
	}

	@Override
	public String getPreparedVariableSymbol() {
		return base.getPreparedVariableSymbol();
	}

	@Override
	public boolean isColumnNamesCaseSensitive() {
		return base.isColumnNamesCaseSensitive();
	}

	@Override
	public String startMultilineComment() {
		return base.startMultilineComment();
	}

	@Override
	public String endMultilineComment() {
		return base.endMultilineComment();
	}

	@Override
	public String beginValueClause() {
		return base.beginValueClause();
	}

	@Override
	public String beginValueSeparatorClause() {
		return base.beginValueSeparatorClause();
	}

	@Override
	public Object endValueClause() {
		return base.endValueClause();
	}

	@Override
	public String getValuesClauseValueSeparator() {
		return base.getValuesClauseValueSeparator();
	}

	@Override
	public String getValuesClauseColumnSeparator() {
		return base.getValuesClauseColumnSeparator();
	}

	@Override
	public String beginTableAlias() {
		return base.beginTableAlias();
	}

	@Override
	public String endTableAlias() {
		return base.endTableAlias();
	}

	@Override
	public String getTableAlias(RowDefinition tabRow) {
		return base.getTableAlias(tabRow);
	}

	@Override
	public String formatTableAlias(String suggestedTableAlias) {
		return base.formatTableAlias(suggestedTableAlias);
	}

	@Override
	protected String getCurrentDateOnlyFunctionName() {
		return base.getCurrentDateOnlyFunctionName();
	}

	@Override
	protected String getCurrentDateTimeFunction() {
		return base.getCurrentDateTimeFunction();
	}

	@Override
	protected String getCurrentZonedDateTimeFunction() {
		return base.getCurrentZonedDateTimeFunction();
	}

	@Override
	public String doCurrentDateTimeTransform() {
		return base.doCurrentDateTimeTransform();
	}

	@Override
	public String getDefaultTimeZoneSign() {
		return base.getDefaultTimeZoneSign();
	}

	@Override
	public String getDefaultTimeZoneHour() {
		return base.getDefaultTimeZoneHour();
	}

	@Override
	public String getDefaultTimeZoneMinute() {
		return base.getDefaultTimeZoneMinute();
	}

	@Override
	protected String getCurrentTimeFunction() {
		return base.getCurrentTimeFunction();
	}

	@Override
	public String doCurrentTimeTransform() {
		return base.doCurrentTimeTransform();
	}

	@Override
	public String doCurrentUTCTimeTransform() {
		return base.doCurrentUTCTimeTransform();
	}

	@Override
	public String getDropDatabase(String databaseName) throws UnsupportedOperationException {
		return base.getDropDatabase(databaseName);
	}

	@Override
	public String doLeftTrimTransform(String enclosedValue) {
		return base.doLeftTrimTransform(enclosedValue);
	}

	@Override
	public String doLowercaseTransform(String enclosedValue) {
		return base.doLowercaseTransform(enclosedValue);
	}

	@Override
	public String doRightTrimTransform(String enclosedValue) {
		return base.doRightTrimTransform(enclosedValue);
	}

	@Override
	public String doStringLengthTransform(String enclosedValue) {
		return base.doStringLengthTransform(enclosedValue);
	}

	@Override
	public String doTrimFunction(String enclosedValue) {
		return base.doTrimFunction(enclosedValue);
	}

	@Override
	public String doUppercaseTransform(String enclosedValue) {
		return base.doUppercaseTransform(enclosedValue);
	}

	@Override
	public String doConcatTransform(String firstString, String secondString) {
		return base.doConcatTransform(firstString, secondString);
	}

	@Override
	public String doConcatTransform(String firstString, String secondString, String... rest) {
		return base.doConcatTransform(firstString, secondString, rest);
	}

	@Override
	public String getNextSequenceValueFunctionName() {
		return base.getNextSequenceValueFunctionName();
	}

	@Override
	public String getRightTrimFunctionName() {
		return base.getRightTrimFunctionName();
	}

	@Override
	public String getLowercaseFunctionName() {
		return base.getLowercaseFunctionName();
	}

	@Override
	public String getUppercaseFunctionName() {
		return base.getUppercaseFunctionName();
	}

	@Override
	public String getStringLengthFunctionName() {
		return base.getStringLengthFunctionName();
	}

	@Override
	public String getCurrentUserFunctionName() {
		return base.getCurrentUserFunctionName();
	}

	@Override
	public String doYearTransform(String dateExpression) {
		return base.doYearTransform(dateExpression);
	}

	@Override
	public String doMonthTransform(String dateExpression) {
		return base.doMonthTransform(dateExpression);
	}

	@Override
	public String doDayTransform(String dateExpression) {
		return base.doDayTransform(dateExpression);
	}

	@Override
	public String doHourTransform(String dateExpression) {
		return base.doHourTransform(dateExpression);
	}

	@Override
	public String doMinuteTransform(String dateExpression) {
		return base.doMinuteTransform(dateExpression);
	}

	@Override
	public String doSecondTransform(String dateExpression) {
		return base.doSecondTransform(dateExpression);
	}

	@Override
	public String doSubsecondTransform(String dateExpression) {
		return base.doSubsecondTransform(dateExpression);
	}

	@Override
	public String doComparableInstantTransform(String instantExpression) {
		return base.doComparableInstantTransform(instantExpression);
	}

	@Override
	public String doInstantYearTransform(String dateExpression) {
		return base.doInstantYearTransform(dateExpression);
	}

	@Override
	public String doInstantMonthTransform(String dateExpression) {
		return base.doInstantMonthTransform(dateExpression);
	}

	@Override
	public String doInstantDayTransform(String dateExpression) {
		return base.doInstantDayTransform(dateExpression);
	}

	@Override
	public String doInstantHourTransform(String dateExpression) {
		return base.doInstantHourTransform(dateExpression);
	}

	@Override
	public String doInstantMinuteTransform(String dateExpression) {
		return base.doInstantMinuteTransform(dateExpression);
	}

	@Override
	public String doInstantSecondTransform(String dateExpression) {
		return base.doInstantSecondTransform(dateExpression);
	}

	@Override
	public String doInstantSubsecondTransform(String dateExpression) {
		return base.doInstantSubsecondTransform(dateExpression);
	}

	@Override
	public String doPositionInStringTransform(String originalString, String stringToFind) {
		return base.doPositionInStringTransform(originalString, stringToFind);
	}

	@Override
	public String getIfNullFunctionName() {
		return base.getIfNullFunctionName();
	}

	@Override
	public boolean supportsComparingBooleanResults() {
		return base.supportsComparingBooleanResults();
	}

	@Override
	public String getNegationFunctionName() {
		return base.getNegationFunctionName();
	}

	@Override
	public String getSubsequentGroupBySubClauseSeparator() {
		return base.getSubsequentGroupBySubClauseSeparator();
	}

	@Override
	public String beginGroupByClause() {
		return base.beginGroupByClause();
	}

	@Override
	public String getAverageFunctionName() {
		return base.getAverageFunctionName();
	}

	@Override
	public String getCountFunctionName() {
		return base.getCountFunctionName();
	}

	@Override
	public String getMaxFunctionName() {
		return base.getMaxFunctionName();
	}

	@Override
	public String getMinFunctionName() {
		return base.getMinFunctionName();
	}

	@Override
	public String getSumFunctionName() {
		return base.getSumFunctionName();
	}

	@Override
	public String getStandardDeviationFunctionName() {
		return base.getStandardDeviationFunctionName();
	}

	@Override
	public boolean prefersIndexBasedOrderByClause() {
		return base.prefersIndexBasedOrderByClause();
	}

	@Override
	public boolean supportsPagingNatively(QueryOptions options) {
		return base.supportsPagingNatively(options);
	}

	@Override
	public boolean supportsGeneratedKeys() {
		return base.supportsGeneratedKeys();
	}

	@Override
	public String getTruncFunctionName() {
		return base.getTruncFunctionName();
	}

	@Override
	public String doTruncTransform(String realNumberExpression, String numberOfDecimalPlacesExpression) {
		return base.doTruncTransform(realNumberExpression, numberOfDecimalPlacesExpression);
	}

	@Override
	public String doStringEqualsTransform(String firstSQLExpression, String secondSQLExpression) {
		return base.doStringEqualsTransform(firstSQLExpression, secondSQLExpression);
	}

	@Override
	public String doBooleanToIntegerTransform(String booleanExpression) {
		return base.doBooleanToIntegerTransform(booleanExpression);
	}

	@Override
	public String doIntegerToBitTransform(String bitExpression) {
		return base.doIntegerToBitTransform(bitExpression);
	}

	@Override
	public String getColumnAutoIncrementSuffix() {
		return base.getColumnAutoIncrementSuffix();
	}

	@Override
	public boolean prefersTriggerBasedIdentities() {
		return base.prefersTriggerBasedIdentities();
	}

	@Override
	public List<String> getTriggerBasedIdentitySQL(DBDatabase db, String table, String column) {
		return base.getTriggerBasedIdentitySQL(db, table, column);
	}

	@Override
	public List<String> dropTriggerBasedIdentitySQL(DBDatabase db, String table, String column) {
		return base.dropTriggerBasedIdentitySQL(db, table, column);
	}

	@Override
	public String getPrimaryKeySequenceName(String table, String column) {
		return base.getPrimaryKeySequenceName(table, column);
	}

	@Override
	public String getPrimaryKeyTriggerName(String table, String column) {
		return base.getPrimaryKeyTriggerName(table, column);
	}

	@Override
	protected boolean hasSpecialAutoIncrementType() {
		return base.hasSpecialAutoIncrementType();
	}

	@Override
	protected boolean propertyWrapperConformsToAutoIncrementType(QueryableDatatype<?> qdt) {
		return base.propertyWrapperConformsToAutoIncrementType(qdt);
	}

	@Override
	protected String getSpecialAutoIncrementType() {
		return base.getSpecialAutoIncrementType();
	}

	@Override
	public boolean prefersTrailingPrimaryKeyDefinition() {
		return base.prefersTrailingPrimaryKeyDefinition();
	}

	@Override
	public boolean prefersLargeObjectsReadAsBase64CharacterStream(DBLargeObject<?> lob) {
		return base.prefersLargeObjectsReadAsBase64CharacterStream(lob);
	}

	@Override
	public boolean prefersLargeObjectsReadAsBytes(DBLargeObject<?> lob) {
		return base.prefersLargeObjectsReadAsBytes(lob);
	}

	@Override
	public boolean prefersLargeObjectsReadAsCLOB(DBLargeObject<?> lob) {
		return base.prefersLargeObjectsReadAsCLOB(lob);
	}

	@Override
	public boolean prefersLargeObjectsReadAsBLOB(DBLargeObject<?> lob) {
		return base.prefersLargeObjectsReadAsBLOB(lob);
	}

	@Override
	public String doSubstringTransform(String originalString, String start, String length) {
		return base.doSubstringTransform(originalString, start, length);
	}

	@Override
	public boolean prefersLargeObjectsSetAsCharacterStream(DBLargeObject<?> lob) {
		return base.prefersLargeObjectsSetAsCharacterStream(lob);
	}

	@Override
	public boolean prefersLargeObjectsSetAsBLOB(DBLargeObject<?> lob) {
		return base.prefersLargeObjectsSetAsBLOB(lob);
	}

	@Override
	public boolean prefersLargeObjectsSetAsBase64String(DBLargeObject<?> lob) {
		return base.prefersLargeObjectsSetAsBase64String(lob);
	}

	@Override
	public String getGreatestOfFunctionName() {
		return base.getGreatestOfFunctionName();
	}

	@Override
	public String getLeastOfFunctionName() {
		return base.getLeastOfFunctionName();
	}

	@Override
	public String getCheezBurger() {
		return base.getCheezBurger();
	}

	@Override
	public boolean prefersDatesReadAsStrings() {
		return base.prefersDatesReadAsStrings();
	}

	@Override
	public boolean prefersInstantsReadAsStrings() {
		return base.prefersInstantsReadAsStrings();
	}

	@Override
	public Date parseDateFromGetString(String getStringDate) throws ParseException {
		return base.parseDateFromGetString(getStringDate);
	}

	@Override
	public LocalDate parseLocalDateFromGetString(String getStringDate) throws ParseException {
		return base.parseLocalDateFromGetString(getStringDate);
	}

	@Override
	public LocalDateTime parseLocalDateTimeFromGetString(String inputFromResultSet) throws ParseException {
		return base.parseLocalDateTimeFromGetString(inputFromResultSet);
	}

	@Override
	public Instant parseInstantFromGetString(String inputFromResultSet) throws ParseException {
		return base.parseInstantFromGetString(inputFromResultSet);
	}

	@Override
	public void sanityCheckDBTableField(DBTableField dbTableField) {
		base.sanityCheckDBTableField(dbTableField);
	}

	@Override
	public boolean supportsRetrievingLastInsertedRowViaSQL() {
		return base.supportsRetrievingLastInsertedRowViaSQL();
	}

	@Override
	public String getRetrieveLastInsertedRowSQL() {
		return base.getRetrieveLastInsertedRowSQL();
	}

	@Override
	public String getEmptyString() {
		return base.getEmptyString();
	}

	@Override
	public boolean supportsDegreesFunction() {
		return base.supportsDegreesFunction();
	}

	@Override
	public boolean supportsRadiansFunction() {
		return base.supportsRadiansFunction();
	}

	@Override
	public String doRadiansTransform(String degreesSQL) {
		return base.doRadiansTransform(degreesSQL);
	}

	@Override
	public String doDegreesTransform(String radiansSQL) {
		return base.doDegreesTransform(radiansSQL);
	}

	@Override
	public String getExpFunctionName() {
		return base.getExpFunctionName();
	}

	@Override
	public boolean supportsExpFunction() {
		return base.supportsExpFunction();
	}

	@Override
	public boolean supportsStandardDeviationFunction() {
		return base.supportsStandardDeviationFunction();
	}

	@Override
	public boolean supportsModulusFunction() {
		return base.supportsModulusFunction();
	}

	@Override
	public String doModulusTransform(String firstNumber, String secondNumber) {
		return base.doModulusTransform(firstNumber, secondNumber);
	}

	@Override
	public String doDateAddSecondsTransform(String dateValue, String numberOfSeconds) {
		return base.doDateAddSecondsTransform(dateValue, numberOfSeconds);
	}

	@Override
	public String doDateAddMinutesTransform(String dateValue, String numberOfMinutes) {
		return base.doDateAddMinutesTransform(dateValue, numberOfMinutes);
	}

	@Override
	public String doDateAddDaysTransform(String dateValue, String numberOfDays) {
		return base.doDateAddDaysTransform(dateValue, numberOfDays);
	}

	@Override
	public String doDateAddHoursTransform(String dateValue, String numberOfHours) {
		return base.doDateAddHoursTransform(dateValue, numberOfHours);
	}

	@Override
	public String doDateAddWeeksTransform(String dateValue, String numberOfWeeks) {
		return base.doDateAddWeeksTransform(dateValue, numberOfWeeks);
	}

	@Override
	public String doDateAddMonthsTransform(String dateValue, String numberOfMonths) {
		return base.doDateAddMonthsTransform(dateValue, numberOfMonths);
	}

	@Override
	public String doDateAddYearsTransform(String dateValue, String numberOfYears) {
		return base.doDateAddYearsTransform(dateValue, numberOfYears);
	}

	@Override
	public String doInstantAddSecondsTransform(String InstantValue, String numberOfSeconds) {
		return base.doInstantAddSecondsTransform(InstantValue, numberOfSeconds);
	}

	@Override
	public String doInstantAddMinutesTransform(String instantValue, String numberOfMinutes) {
		return base.doInstantAddMinutesTransform(instantValue, numberOfMinutes);
	}

	@Override
	public String doInstantAddDaysTransform(String instantValue, String numberOfDays) {
		return base.doInstantAddDaysTransform(instantValue, numberOfDays);
	}

	@Override
	public String doInstantAddHoursTransform(String instantValue, String numberOfHours) {
		return base.doInstantAddHoursTransform(instantValue, numberOfHours);
	}

	@Override
	public String doInstantAddWeeksTransform(String instantValue, String numberOfWeeks) {
		return base.doInstantAddWeeksTransform(instantValue, numberOfWeeks);
	}

	@Override
	public String doInstantAddMonthsTransform(String instantValue, String numberOfMonths) {
		return base.doInstantAddMonthsTransform(instantValue, numberOfMonths);
	}

	@Override
	public String doInstantAddYearsTransform(String instantValue, String numberOfYears) {
		return base.doInstantAddYearsTransform(instantValue, numberOfYears);
	}

	@Override
	public String doBooleanValueTransform(Boolean boolValue) {
		return base.doBooleanValueTransform(boolValue);
	}

	@Override
	public boolean supportsXOROperator() {
		return base.supportsXOROperator();
	}

	@Override
	public String doLeastOfTransformation(List<String> strs) {
		return base.doLeastOfTransformation(strs);
	}

	@Override
	public String doGreatestOfTransformation(List<String> strs) {
		return base.doGreatestOfTransformation(strs);
	}

	@Override
	public String doReplaceTransform(String withinString, String findString, String replaceString) {
		return base.doReplaceTransform(withinString, findString, replaceString);
	}

	@Override
	protected String doNumberToStringTransformUnsafe(String numberExpression) {
		return base.doNumberToStringTransformUnsafe(numberExpression);
	}

	@Override
	protected String doIntegerToStringTransformUnsafe(String integerExpression) {
		return base.doIntegerToStringTransformUnsafe(integerExpression);
	}

	@Override
	public String doCurrentDateOnlyTransform() {
		return base.doCurrentDateOnlyTransform();
	}

	@Override
	public String doBitsValueTransform(boolean[] booleanArray) {
		return base.doBitsValueTransform(booleanArray);
	}

	@Override
	public String doDayDifferenceTransform(String dateValue, String otherDateValue) {
		return base.doDayDifferenceTransform(dateValue, otherDateValue);
	}

	@Override
	public String doWeekDifferenceTransform(String dateValue, String otherDateValue) {
		return base.doWeekDifferenceTransform(dateValue, otherDateValue);
	}

	@Override
	public String doMonthDifferenceTransform(String dateValue, String otherDateValue) {
		return base.doMonthDifferenceTransform(dateValue, otherDateValue);
	}

	@Override
	public String doYearDifferenceTransform(String dateValue, String otherDateValue) {
		return base.doYearDifferenceTransform(dateValue, otherDateValue);
	}

	@Override
	public String doHourDifferenceTransform(String dateValue, String otherDateValue) {
		return base.doHourDifferenceTransform(dateValue, otherDateValue);
	}

	@Override
	public String doMinuteDifferenceTransform(String dateValue, String otherDateValue) {
		return base.doMinuteDifferenceTransform(dateValue, otherDateValue);
	}

	@Override
	public String doSecondDifferenceTransform(String dateValue, String otherDateValue) {
		return base.doSecondDifferenceTransform(dateValue, otherDateValue);
	}

	@Override
	public String getForeignKeyClauseForCreateTable(PropertyWrapper<?, ?, ?> field) {
		return base.getForeignKeyClauseForCreateTable(field);
	}

	@Override
	public String doStringIfNullTransform(String possiblyNullValue, String alternativeIfNull) {
		return base.doStringIfNullTransform(possiblyNullValue, alternativeIfNull);
	}

	@Override
	public String doStringIfNullUseEmptyStringTransform(String possiblyNullValue) {
		return base.doStringIfNullUseEmptyStringTransform(possiblyNullValue);
	}

	@Override
	public String doNumberIfNullTransform(String possiblyNullValue, String alternativeIfNull) {
		return base.doNumberIfNullTransform(possiblyNullValue, alternativeIfNull);
	}

	@Override
	public String doIntegerIfNullTransform(String possiblyNullValue, String alternativeIfNull) {
		return base.doIntegerIfNullTransform(possiblyNullValue, alternativeIfNull);
	}

	@Override
	public String doDateIfNullTransform(String possiblyNullValue, String alternativeIfNull) {
		return base.doDateIfNullTransform(possiblyNullValue, alternativeIfNull);
	}

	@Override
	public String doInTransform(String comparableValue, List<String> values) {
		return base.doInTransform(comparableValue, values);
	}

	@Override
	public String doNotInTransform(String comparableValue, List<String> values) {
		return base.doNotInTransform(comparableValue, values);
	}

	@Override
	public String getFromClause(DBRow table) {
		return base.getFromClause(table);
	}

	@Override
	public String beginWithClause() {
		return base.beginWithClause();
	}

	@Override
	public String formatWithClauseTableDefinition(String recursiveTableAlias, String recursiveColumnNames) {
		return base.formatWithClauseTableDefinition(recursiveTableAlias, recursiveColumnNames);
	}

	@Override
	public String beginWithClausePrimingQuery() {
		return base.beginWithClausePrimingQuery();
	}

	@Override
	public String endWithClausePrimingQuery() {
		return base.endWithClausePrimingQuery();
	}

	@Override
	public String beginWithClauseRecursiveQuery() {
		return base.beginWithClauseRecursiveQuery();
	}

	@Override
	public String endWithClauseRecursiveQuery() {
		return base.endWithClauseRecursiveQuery();
	}

	@Override
	public String doSelectFromRecursiveTable(String recursiveTableAlias, String recursiveAliases) {
		return base.doSelectFromRecursiveTable(recursiveTableAlias, recursiveAliases);
	}

	@Override
	public boolean requiresRecursiveTableAlias() {
		return base.requiresRecursiveTableAlias();
	}

	@Override
	public String getRecursiveQueryDepthColumnName() {
		return base.getRecursiveQueryDepthColumnName();
	}

	@Override
	protected boolean hasSpecialPrimaryKeyTypeForDBDatatype(PropertyWrapper<?, ?, ?> field) {
		return base.hasSpecialPrimaryKeyTypeForDBDatatype(field);
	}

	@Override
	protected String getSpecialPrimaryKeyTypeOfDBDatatype(PropertyWrapper<?, ?, ?> field) {
		return base.getSpecialPrimaryKeyTypeOfDBDatatype(field);
	}

	@Override
	protected boolean supportsLeastOfNatively() {
		return base.supportsLeastOfNatively();
	}

	@Override
	protected boolean supportsGreatestOfNatively() {
		return base.supportsGreatestOfNatively();
	}

	@Override
	public boolean supportsPurelyFunctionalGroupByColumns() {
		return base.supportsPurelyFunctionalGroupByColumns();
	}

	@Override
	public String getSystemTableExclusionPattern() {
		return base.getSystemTableExclusionPattern();
	}

	@Override
	public String formatPrimaryKeyForRetrievingGeneratedKeys(String primaryKeyColumnName) {
		return base.formatPrimaryKeyForRetrievingGeneratedKeys(primaryKeyColumnName);
	}

	@Override
	public String doChooseTransformation(String numberToChooseWith, List<String> strs) {
		return base.doChooseTransformation(numberToChooseWith, strs);
	}

	@Override
	public String getChooseFunctionName() {
		return base.getChooseFunctionName();
	}

	@Override
	protected boolean supportsChooseNatively() {
		return base.supportsChooseNatively();
	}

	@Override
	public String doIfThenElseTransform(String booleanTest, String thenResult, String elseResult) {
		return base.doIfThenElseTransform(booleanTest, thenResult, elseResult);
	}

	@Override
	public String doIfEmptyStringThenElse(String expressionSQL, String ifResult, String thenResult) {
		return base.doIfEmptyStringThenElse(expressionSQL, ifResult, thenResult);
	}

	@Override
	public String doIfNullThenElse(String expressionSQL, String ifResult, String thenResult) {
		return base.doIfNullThenElse(expressionSQL, ifResult, thenResult);
	}

	@Override
	public String doDayOfWeekTransform(String dateSQL) {
		return base.doDayOfWeekTransform(dateSQL);
	}

	@Override
	public String doInstantDayOfWeekTransform(String dateSQL) {
		return base.doInstantDayOfWeekTransform(dateSQL);
	}

	@Override
	public String getIndexClauseForCreateTable(PropertyWrapper<?, ?, ?> field) {
		return base.getIndexClauseForCreateTable(field);
	}

	@Override
	public String doBooleanArrayTransform(Boolean[] bools) {
		return base.doBooleanArrayTransform(bools);
	}

	@Override
	public Boolean[] doBooleanArrayResultInterpretation(String stringOfBools) {
		return base.doBooleanArrayResultInterpretation(stringOfBools);
	}

	@Override
	public boolean supportsArraysNatively() {
		return base.supportsArraysNatively();
	}

	@Override
	public Boolean doBooleanArrayElementTransform(Object objRepresentingABoolean) {
		return base.doBooleanArrayElementTransform(objRepresentingABoolean);
	}

	@Override
	public String doNumberEqualsTransform(String leftHandSide, String rightHandSide) {
		return base.doNumberEqualsTransform(leftHandSide, rightHandSide);
	}

	@Override
	public String doIntegerEqualsTransform(String leftHandSide, String rightHandSide) {
		return base.doIntegerEqualsTransform(leftHandSide, rightHandSide);
	}

	@Override
	public String getAlterTableAddForeignKeyStatement(DBRow newTableRow, PropertyWrapper<?, ?, ?> field) {
		return base.getAlterTableAddForeignKeyStatement(newTableRow, field);
	}

	@Override
	public String getAlterTableDropForeignKeyStatement(DBRow newTableRow, PropertyWrapper<?, ?, ?> field) {
		return base.getAlterTableDropForeignKeyStatement(newTableRow, field);
	}

	@Override
	public String doColumnTransformForSelect(QueryableDatatype<?> qdt, String selectableName) {
		String result = base.doColumnTransformForSelect(qdt, selectableName);
		if ((qdt instanceof DBString) // if it's a string 
				&& requiredToProduceEmptyStringsForNull() // and it needs to be compatible with Oracle
				&& base.supportsDifferenceBetweenNullAndEmptyStringNatively() // and it isn't compatible normally
				) {
			result = convertNullToEmptyString(result); // make all the nulls into empty strings
		}
		return result;
	}

	@Override
	public String transformPeriodIntoDateRepeat(Period interval) {
		return base.transformPeriodIntoDateRepeat(interval);
	}

	@Override
	public String doDateMinusToDateRepeatTransformation(String leftHandSide, String rightHandSide) {
		return base.doDateMinusToDateRepeatTransformation(leftHandSide, rightHandSide);
	}

	@Override
	public String doDateRepeatEqualsTransform(String leftHandSide, String rightHandSide) {
		return base.doDateRepeatEqualsTransform(leftHandSide, rightHandSide);
	}

	@Override
	public String doDateRepeatNotEqualsTransform(String leftHandSide, String rightHandSide) {
		return base.doDateRepeatNotEqualsTransform(leftHandSide, rightHandSide);
	}

	@Override
	public String doDateRepeatLessThanTransform(String leftHandSide, String rightHandSide) {
		return base.doDateRepeatLessThanTransform(leftHandSide, rightHandSide);
	}

	@Override
	public String doDateRepeatLessThanEqualsTransform(String leftHandSide, String rightHandSide) {
		return base.doDateRepeatLessThanEqualsTransform(leftHandSide, rightHandSide);
	}

	@Override
	public String doDateRepeatGreaterThanTransform(String leftHandSide, String rightHandSide) {
		return base.doDateRepeatGreaterThanTransform(leftHandSide, rightHandSide);
	}

	@Override
	public String doDateRepeatGreaterThanEqualsTransform(String leftHandSide, String rightHandSide) {
		return base.doDateRepeatGreaterThanEqualsTransform(leftHandSide, rightHandSide);
	}

	@Override
	public String doDatePlusDateRepeatTransform(String leftHandSide, String rightHandSide) {
		return base.doDatePlusDateRepeatTransform(leftHandSide, rightHandSide);
	}

	@Override
	public String doDateMinusDateRepeatTransform(String leftHandSide, String rightHandSide) {
		return base.doDateMinusDateRepeatTransform(leftHandSide, rightHandSide);
	}

	@Override
	public Period parseDateRepeatFromGetString(String intervalStr) {
		return base.parseDateRepeatFromGetString(intervalStr);
	}

	@Override
	public String doPolygon2DEqualsTransform(String firstGeometry, String secondGeometry) {
		return base.doPolygon2DEqualsTransform(firstGeometry, secondGeometry);
	}

	@Override
	public String doPolygon2DUnionTransform(String firstGeometry, String secondGeometry) {
		return base.doPolygon2DUnionTransform(firstGeometry, secondGeometry);
	}

	@Override
	public String doPolygon2DIntersectionTransform(String firstGeometry, String secondGeometry) {
		return base.doPolygon2DIntersectionTransform(firstGeometry, secondGeometry);
	}

	@Override
	public String doPolygon2DIntersectsTransform(String firstGeometry, String secondGeometry) {
		return base.doPolygon2DIntersectsTransform(firstGeometry, secondGeometry);
	}

	@Override
	public String doPolygon2DContainsPolygon2DTransform(String firstGeometry, String secondGeometry) {
		return base.doPolygon2DContainsPolygon2DTransform(firstGeometry, secondGeometry);
	}

	@Override
	public String doPolygon2DDoesNotIntersectTransform(String firstGeometry, String secondGeometry) {
		return base.doPolygon2DDoesNotIntersectTransform(firstGeometry, secondGeometry);
	}

	@Override
	public String doPolygon2DOverlapsTransform(String firstGeometry, String secondGeometry) {
		return base.doPolygon2DOverlapsTransform(firstGeometry, secondGeometry);
	}

	@Override
	public String doPolygon2DTouchesTransform(String firstGeometry, String secondGeometry) {
		return base.doPolygon2DTouchesTransform(firstGeometry, secondGeometry);
	}

	@Override
	public String doPolygon2DWithinTransform(String firstGeometry, String secondGeometry) {
		return base.doPolygon2DWithinTransform(firstGeometry, secondGeometry);
	}

	@Override
	public String doPolygon2DMeasurableDimensionsTransform(String polygon2DSQL) {
		return base.doPolygon2DMeasurableDimensionsTransform(polygon2DSQL);
	}

	@Override
	public String doPolygon2DGetBoundingBoxTransform(String polygon2DSQL) {
		return base.doPolygon2DGetBoundingBoxTransform(polygon2DSQL);
	}

	@Override
	public String doPolygon2DGetAreaTransform(String polygon2DSQL) {
		return base.doPolygon2DGetAreaTransform(polygon2DSQL);
	}

	@Override
	public String doPolygon2DGetExteriorRingTransform(String polygon2DSQL) {
		return base.doPolygon2DGetExteriorRingTransform(polygon2DSQL);
	}

	@Override
	public boolean supportsHyperbolicFunctionsNatively() {
		return base.supportsHyperbolicFunctionsNatively();
	}

	@Override
	public String getArctan2FunctionName() {
		return base.getArctan2FunctionName();
	}

	@Override
	public String doDateRepeatGetYearsTransform(String dateRepeatSQL) {
		return base.doDateRepeatGetYearsTransform(dateRepeatSQL);
	}

	@Override
	public String doDateRepeatGetMonthsTransform(String dateRepeatSQL) {
		return base.doDateRepeatGetMonthsTransform(dateRepeatSQL);
	}

	@Override
	public String doDateRepeatGetDaysTransform(String dateRepeatSQL) {
		return base.doDateRepeatGetDaysTransform(dateRepeatSQL);
	}

	@Override
	public String doDateRepeatGetHoursTransform(String dateRepeatSQL) {
		return base.doDateRepeatGetHoursTransform(dateRepeatSQL);
	}

	@Override
	public String doDateRepeatGetMinutesTransform(String dateRepeatSQL) {
		return base.doDateRepeatGetMinutesTransform(dateRepeatSQL);
	}

	@Override
	public String doDateRepeatGetSecondsTransform(String dateRepeatSQL) {
		return base.doDateRepeatGetSecondsTransform(dateRepeatSQL);
	}

	@Override
	public String doDateRepeatToStringTransform(String dateRepeatSQL) {
		return base.doDateRepeatToStringTransform(dateRepeatSQL);
	}

	@Override
	public String doStringToNumberTransform(String stringResultContainingANumber) {
		return base.doStringToNumberTransform(stringResultContainingANumber);
	}

	@Override
	public boolean supportsArcSineFunction() {
		return base.supportsArcSineFunction();
	}

	@Override
	public boolean supportsCotangentFunction() {
		return base.supportsCotangentFunction();
	}

	@Override
	public DBExpression transformToStorableType(DBExpression columnExpression) {
		return base.transformToStorableType(columnExpression);
	}

	@Override
	public DBExpression transformToSortableType(DBExpression columnExpression) {
		return base.transformToSortableType(columnExpression);
	}

	@Override
	public String doPoint2DEqualsTransform(String firstPoint, String secondPoint) {
		return base.doPoint2DEqualsTransform(firstPoint, secondPoint);
	}

	@Override
	public String doPoint2DGetXTransform(String pont2DSQL) {
		return base.doPoint2DGetXTransform(pont2DSQL);
	}

	@Override
	public String doPoint2DGetYTransform(String point2DSQL) {
		return base.doPoint2DGetYTransform(point2DSQL);
	}

	@Override
	public String doPoint2DMeasurableDimensionsTransform(String point2DSQL) {
		return base.doPoint2DMeasurableDimensionsTransform(point2DSQL);
	}

	@Override
	public String doPoint2DGetBoundingBoxTransform(String point2DSQL) {
		return base.doPoint2DGetBoundingBoxTransform(point2DSQL);
	}

	@Override
	public String doPoint2DAsTextTransform(String point2DSQL) {
		return base.doPoint2DAsTextTransform(point2DSQL);
	}

	@Override
	public String transformPoint2DIntoDatabaseFormat(Point point) {
		return base.transformPoint2DIntoDatabaseFormat(point);
	}

	@Override
	public String transformCoordinatesIntoDatabasePoint2DFormat(String xValue, String yValue) {
		return base.transformCoordinatesIntoDatabasePoint2DFormat(xValue, yValue);
	}

	@Override
	public Point transformDatabasePoint2DValueToJTSPoint(String pointAsString) throws com.vividsolutions.jts.io.ParseException {
		return base.transformDatabasePoint2DValueToJTSPoint(pointAsString);
	}

	@Override
	public Polygon transformDatabasePolygon2DToJTSPolygon(String polygon2DSQL) throws com.vividsolutions.jts.io.ParseException {
		return base.transformDatabasePolygon2DToJTSPolygon(polygon2DSQL);
	}

	@Override
	public LineString transformDatabaseLine2DValueToJTSLineString(String lineStringAsSQL) throws com.vividsolutions.jts.io.ParseException {
		return base.transformDatabaseLine2DValueToJTSLineString(lineStringAsSQL);
	}

	@Override
	public String transformLineStringIntoDatabaseLine2DFormat(LineString lineString) {
		return base.transformLineStringIntoDatabaseLine2DFormat(lineString);
	}

	@Override
	public String doLine2DAsTextTransform(String line2DSQL) {
		return base.doLine2DAsTextTransform(line2DSQL);
	}

	@Override
	public String doLine2DEqualsTransform(String line2DSQL, String otherLine2DSQL) {
		return base.doLine2DEqualsTransform(line2DSQL, otherLine2DSQL);
	}

	@Override
	public String doLine2DNotEqualsTransform(String line2DSQL, String otherLine2DSQL) {
		return base.doLine2DNotEqualsTransform(line2DSQL, otherLine2DSQL);
	}

	@Override
	public String doLine2DMeasurableDimensionsTransform(String line2DSQL) {
		return base.doLine2DMeasurableDimensionsTransform(line2DSQL);
	}

	@Override
	public String doLine2DGetBoundingBoxTransform(String line2DSQL) {
		return base.doLine2DGetBoundingBoxTransform(line2DSQL);
	}

	@Override
	public String transformPoint2DArrayToDatabasePolygon2DFormat(List<String> pointSQL) {
		return base.transformPoint2DArrayToDatabasePolygon2DFormat(pointSQL);
	}

	@Override
	public String doLine2DGetMaxXTransform(String line2DSQL) {
		return base.doLine2DGetMaxXTransform(line2DSQL);
	}

	@Override
	public String doLine2DGetMinXTransform(String line2DSQL) {
		return base.doLine2DGetMinXTransform(line2DSQL);
	}

	@Override
	public String doLine2DGetMaxYTransform(String line2DSQL) {
		return base.doLine2DGetMaxYTransform(line2DSQL);
	}

	@Override
	public String doLine2DGetMinYTransform(String line2DSQL) {
		return base.doLine2DGetMinYTransform(line2DSQL);
	}

	@Override
	public String doPolygon2DGetMaxXTransform(String polygon2DSQL) {
		return base.doPolygon2DGetMaxXTransform(polygon2DSQL);
	}

	@Override
	public String doPolygon2DGetMinXTransform(String polygon2DSQL) {
		return base.doPolygon2DGetMinXTransform(polygon2DSQL);
	}

	@Override
	public String doPolygon2DGetMaxYTransform(String polygon2DSQL) {
		return base.doPolygon2DGetMaxYTransform(polygon2DSQL);
	}

	@Override
	public String doPolygon2DGetMinYTransform(String polygon2DSQL) {
		return base.doPolygon2DGetMinYTransform(polygon2DSQL);
	}

	@Override
	public String transformPolygonIntoDatabasePolygon2DFormat(Polygon polygon2D) {
		return base.transformPolygonIntoDatabasePolygon2DFormat(polygon2D);
	}

	@Override
	public String doPoint2DDistanceBetweenTransform(String polygon2DSQL, String otherPolygon2DSQL) {
		return base.doPoint2DDistanceBetweenTransform(polygon2DSQL, otherPolygon2DSQL);
	}

	@Override
	public String doRoundTransform(String numberSQL) {
		return base.doRoundTransform(numberSQL);
	}

	@Override
	public String doRoundWithDecimalPlacesTransform(String number, String decimalPlaces) {
		return base.doRoundWithDecimalPlacesTransform(number, decimalPlaces);
	}

	@Override
	public String doSubstringBeforeTransform(String fromThis, String beforeThis) {
		return base.doSubstringBeforeTransform(fromThis, beforeThis);
	}

	@Override
	public String doSubstringAfterTransform(String fromThis, String afterThis) {
		return base.doSubstringAfterTransform(fromThis, afterThis);
	}

	@Override
	public boolean willCloseConnectionOnStatementCancel() {
		return base.willCloseConnectionOnStatementCancel();
	}

	@Override
	public boolean supportsStatementIsClosed() {
		return base.supportsStatementIsClosed();
	}

	@Override
	public String doPolygon2DContainsPoint2DTransform(String polygon2DSQL, String point2DSQL) {
		return base.doPolygon2DContainsPoint2DTransform(polygon2DSQL, point2DSQL);
	}

	@Override
	public String doPolygon2DAsTextTransform(String polygonSQL) {
		return base.doPolygon2DAsTextTransform(polygonSQL);
	}

	@Override
	public String doLine2DIntersectsLine2DTransform(String firstLine, String secondLine) {
		return base.doLine2DIntersectsLine2DTransform(firstLine, secondLine);
	}

	@Override
	public String doLine2DIntersectionPointWithLine2DTransform(String firstLine, String secondLine) {
		return base.doLine2DIntersectionPointWithLine2DTransform(firstLine, secondLine);
	}

	@Override
	public String doLine2DAllIntersectionPointsWithLine2DTransform(String firstLine, String secondLine) {
		return base.doLine2DAllIntersectionPointsWithLine2DTransform(firstLine, secondLine);
	}

	@Override
	public LineSegment transformDatabaseLineSegment2DValueToJTSLineSegment(String lineSegmentAsSQL) throws com.vividsolutions.jts.io.ParseException {
		return base.transformDatabaseLineSegment2DValueToJTSLineSegment(lineSegmentAsSQL);
	}

	@Override
	public String transformLineSegmentIntoDatabaseLineSegment2DFormat(LineSegment lineSegment) {
		return base.transformLineSegmentIntoDatabaseLineSegment2DFormat(lineSegment);
	}

	@Override
	public String doLineSegment2DIntersectsLineSegment2DTransform(String firstSQL, String secondSQL) {
		return base.doLineSegment2DIntersectsLineSegment2DTransform(firstSQL, secondSQL);
	}

	@Override
	public String doLineSegment2DGetMaxXTransform(String lineSegment) {
		return base.doLineSegment2DGetMaxXTransform(lineSegment);
	}

	@Override
	public String doLineSegment2DGetMinXTransform(String lineSegment) {
		return base.doLineSegment2DGetMinXTransform(lineSegment);
	}

	@Override
	public String doLineSegment2DGetMaxYTransform(String lineSegment) {
		return base.doLineSegment2DGetMaxYTransform(lineSegment);
	}

	@Override
	public String doLineSegment2DGetMinYTransform(String lineSegment) {
		return base.doLineSegment2DGetMinYTransform(lineSegment);
	}

	@Override
	public String doLineSegment2DGetBoundingBoxTransform(String lineSegment) {
		return base.doLineSegment2DGetBoundingBoxTransform(lineSegment);
	}

	@Override
	public String doLineSegment2DDimensionTransform(String lineSegment) {
		return base.doLineSegment2DDimensionTransform(lineSegment);
	}

	@Override
	public String doLineSegment2DNotEqualsTransform(String firstLineSegment, String secondLineSegment) {
		return base.doLineSegment2DNotEqualsTransform(firstLineSegment, secondLineSegment);
	}

	@Override
	public String doLineSegment2DEqualsTransform(String firstLineSegment, String secondLineSegment) {
		return base.doLineSegment2DEqualsTransform(firstLineSegment, secondLineSegment);
	}

	@Override
	public String doLineSegment2DAsTextTransform(String lineSegment) {
		return base.doLineSegment2DAsTextTransform(lineSegment);
	}

	@Override
	public String doLineSegment2DIntersectionPointWithLineSegment2DTransform(String firstLineSegment, String secondLineSegment) {
		return base.doLineSegment2DIntersectionPointWithLineSegment2DTransform(firstLineSegment, secondLineSegment);
	}

	@Override
	public String doLineSegment2DStartPointTransform(String lineSegmentSQL) {
		return base.doLineSegment2DStartPointTransform(lineSegmentSQL);
	}

	@Override
	public String doLineSegment2DEndPointTransform(String lineSegmentSQL) {
		return base.doLineSegment2DEndPointTransform(lineSegmentSQL);
	}

	@Override
	public String transformMultiPoint2DToDatabaseMultiPoint2DValue(MultiPoint points) {
		return base.transformMultiPoint2DToDatabaseMultiPoint2DValue(points);
	}

	@Override
	public MultiPoint transformDatabaseMultiPoint2DValueToJTSMultiPoint(String pointsAsString) throws com.vividsolutions.jts.io.ParseException {
		return base.transformDatabaseMultiPoint2DValueToJTSMultiPoint(pointsAsString);
	}

	@Override
	public String doMultiPoint2DEqualsTransform(String firstMultiPointValue, String secondMultiPointValue) {
		return base.doMultiPoint2DEqualsTransform(firstMultiPointValue, secondMultiPointValue);
	}

	@Override
	public String doMultiPoint2DNotEqualsTransform(String first, String second) {
		return base.doMultiPoint2DNotEqualsTransform(first, second);
	}

	@Override
	public String doMultiPoint2DGetPointAtIndexTransform(String first, String index) {
		return base.doMultiPoint2DGetPointAtIndexTransform(first, index);
	}

	@Override
	public String doMultiPoint2DGetNumberOfPointsTransform(String multiPoint2D) {
		return base.doMultiPoint2DGetNumberOfPointsTransform(multiPoint2D);
	}

	@Override
	public String doMultiPoint2DMeasurableDimensionsTransform(String multipoint2D) {
		return base.doMultiPoint2DMeasurableDimensionsTransform(multipoint2D);
	}

	@Override
	public String doMultiPoint2DGetBoundingBoxTransform(String multiPoint2D) {
		return base.doMultiPoint2DGetBoundingBoxTransform(multiPoint2D);
	}

	@Override
	public String doMultiPoint2DAsTextTransform(String multiPoint2D) {
		return base.doMultiPoint2DAsTextTransform(multiPoint2D);
	}

	@Override
	public String doMultiPoint2DToLine2DTransform(String multiPoint2D) {
		return base.doMultiPoint2DToLine2DTransform(multiPoint2D);
	}

	@Override
	public String doMultiPoint2DGetMinYTransform(String multiPoint2D) {
		return base.doMultiPoint2DGetMinYTransform(multiPoint2D);
	}

	@Override
	public String doMultiPoint2DGetMinXTransform(String multiPoint2D) {
		return base.doMultiPoint2DGetMinXTransform(multiPoint2D);
	}

	@Override
	public String doMultiPoint2DGetMaxYTransform(String multiPoint2D) {
		return base.doMultiPoint2DGetMaxYTransform(multiPoint2D);
	}

	@Override
	public String doMultiPoint2DGetMaxXTransform(String multiPoint2D) {
		return base.doMultiPoint2DGetMaxXTransform(multiPoint2D);
	}

	@Override
	public boolean supportsRowLimitsNatively(QueryOptions options) {
		return base.supportsRowLimitsNatively(options);
	}

	@Override
	public boolean requiresSpatial2DIndexes() {
		return base.requiresSpatial2DIndexes();
	}

	@Override
	public List<String> getSpatial2DIndexSQL(DBDatabase database, String formatTableName, String formatColumnName) {
		return base.getSpatial2DIndexSQL(database, formatTableName, formatColumnName);
	}

	@Override
	public String doWrapQueryForPaging(String sqlQuery, QueryOptions options) {
		return base.doWrapQueryForPaging(sqlQuery, options);
	}

	@Override
	public String doLine2DSpatialDimensionsTransform(String line2DSQL) {
		return base.doLine2DSpatialDimensionsTransform(line2DSQL);
	}

	@Override
	public String doLine2DHasMagnitudeTransform(String line2DSQL) {
		return base.doLine2DHasMagnitudeTransform(line2DSQL);
	}

	@Override
	public String doLine2DGetMagnitudeTransform(String line2DSQL) {
		return base.doLine2DGetMagnitudeTransform(line2DSQL);
	}

	@Override
	public String doPoint2DSpatialDimensionsTransform(String point2DSQL) {
		return base.doPoint2DSpatialDimensionsTransform(point2DSQL);
	}

	@Override
	public String doPoint2DHasMagnitudeTransform(String point2DSQL) {
		return base.doPoint2DHasMagnitudeTransform(point2DSQL);
	}

	@Override
	public String doPoint2DGetMagnitudeTransform(String point2DSQL) {
		return base.doPoint2DGetMagnitudeTransform(point2DSQL);
	}

	@Override
	public String doMultiPoint2DSpatialDimensionsTransform(String multipoint2DSQL) {
		return base.doMultiPoint2DSpatialDimensionsTransform(multipoint2DSQL);
	}

	@Override
	public String doMultiPoint2DHasMagnitudeTransform(String multipoint2DSQL) {
		return base.doMultiPoint2DHasMagnitudeTransform(multipoint2DSQL);
	}

	@Override
	public String doMultiPoint2DGetMagnitudeTransform(String multipoint2DSQL) {
		return base.doMultiPoint2DGetMagnitudeTransform(multipoint2DSQL);
	}

	@Override
	public String doPolygon2DSpatialDimensionsTransform(String polygon2DSQL) {
		return base.doPolygon2DSpatialDimensionsTransform(polygon2DSQL);
	}

	@Override
	public String doPolygon2DHasMagnitudeTransform(String polygon2DSQL) {
		return base.doPolygon2DHasMagnitudeTransform(polygon2DSQL);
	}

	@Override
	public String doPolygon2DGetMagnitudeTransform(String polygon2DSQL) {
		return base.doPolygon2DGetMagnitudeTransform(polygon2DSQL);
	}

	@Override
	public String doLineSegment2DSpatialDimensionsTransform(String lineSegment2DSQL) {
		return base.doLineSegment2DSpatialDimensionsTransform(lineSegment2DSQL);
	}

	@Override
	public String doLineSegment2DHasMagnitudeTransform(String lineSegment2DSQL) {
		return base.doLineSegment2DHasMagnitudeTransform(lineSegment2DSQL);
	}

	@Override
	public String doLineSegment2DGetMagnitudeTransform(String lineSegment2DSQL) {
		return base.doLineSegment2DGetMagnitudeTransform(lineSegment2DSQL);
	}

	@Override
	public String transformCoordinateArrayToDatabasePolygon2DFormat(List<String> coordinateSQL) {
		return base.transformCoordinateArrayToDatabasePolygon2DFormat(coordinateSQL);
	}

	@Override
	public String doEndOfMonthTransform(String dateSQL) {
		return base.doEndOfMonthTransform(dateSQL);
	}

	@Override
	public String doInstantEndOfMonthTransform(String dateSQL) {
		return base.doInstantEndOfMonthTransform(dateSQL);
	}

	@Override
	public String doDateAtTimeZoneTransform(String dateSQL, TimeZone timeZone) throws UnsupportedOperationException {
		return base.doDateAtTimeZoneTransform(dateSQL, timeZone);
	}

	@Override
	public Class<? extends QueryableDatatype<?>> getQueryableDatatypeClassForSQLDatatype(String typeName) {
		return base.getQueryableDatatypeClassForSQLDatatype(typeName);
	}

	@Override
	public String getHavingClauseStart() {
		return base.getHavingClauseStart();
	}

	@Override
	public String getTrueValue() {
		return base.getTrueValue();
	}

	@Override
	public String getFalseValue() {
		return base.getFalseValue();
	}

	@Override
	public String doBooleanStatementToBooleanComparisonValueTransform(String booleanStatement) {
		return base.doBooleanStatementToBooleanComparisonValueTransform(booleanStatement);
	}

	@Override
	public String doBooleanValueToBooleanComparisonValueTransform(String booleanValueSQL) {
		return base.doBooleanValueToBooleanComparisonValueTransform(booleanValueSQL);
	}

	@Override
	public String getUnionDistinctOperator() {
		return base.getUnionDistinctOperator();
	}

	@Override
	public String getUnionOperator() {
		return base.getUnionOperator();
	}

	@Override
	public LargeObjectHandlerType preferredLargeObjectWriter(DBLargeObject<?> lob) {
		return base.preferredLargeObjectWriter(lob);
	}

	@Override
	public LargeObjectHandlerType preferredLargeObjectReader(DBLargeObject<?> lob) {
		return base.preferredLargeObjectReader(lob);
	}

	@Override
	public String getRoundUpFunctionName() {
		return base.getRoundUpFunctionName();
	}

	@Override
	public String getNaturalLogFunctionName() {
		return base.getNaturalLogFunctionName();
	}

	@Override
	public String getLogBase10FunctionName() {
		return base.getLogBase10FunctionName();
	}

	@Override
	public String doRandomNumberTransform() {
		return base.doRandomNumberTransform();
	}

	@Override
	public String doRandomIntegerTransform() {
		return base.doRandomIntegerTransform();
	}

	@Override
	public String doLogBase10NumberTransform(String sql) {
		return base.doLogBase10NumberTransform(sql);
	}

	@Override
	public String doLogBase10IntegerTransform(String sql) {
		return base.doLogBase10IntegerTransform(sql);
	}

	@Override
	public String doNumberToIntegerTransform(String sql) {
		return base.doNumberToIntegerTransform(sql);
	}

	@Override
	public String doFindNumberInStringTransform(String toSQLString) {
		return base.doFindNumberInStringTransform(toSQLString);
	}

	@Override
	public String doFindIntegerInStringTransform(String toSQLString) {
		return base.doFindIntegerInStringTransform(toSQLString);
	}

	@Override
	public String doIntegerToNumberTransform(String toSQLString) {
		return base.doIntegerToNumberTransform(toSQLString);
	}

	@Override
	public boolean persistentConnectionRequired() {
		return base.persistentConnectionRequired();
	}

	@Override
	public Boolean supportsDifferenceBetweenNullAndEmptyStringNatively() {
		return base.supportsDifferenceBetweenNullAndEmptyStringNatively();
	}

	@Override
	public Boolean supportsUnionDistinct() {
		return base.supportsUnionDistinct();
	}

	@Override
	public boolean supportsRecursiveQueriesNatively() {
		return base.supportsRecursiveQueriesNatively();
	}

	@Override
	public boolean supportsFullOuterJoin() {
		return base.supportsFullOuterJoin();
	}

	@Override
	public boolean supportsFullOuterJoinNatively() {
		return base.supportsFullOuterJoinNatively();
	}

	@Override
	public boolean supportsRightOuterJoinNatively() {
		return base.supportsRightOuterJoinNatively();
	}

	@Override
	boolean supportsPaging(QueryOptions options) {
		return base.supportsPaging(options);
	}

	@Override
	public boolean supportsAlterTableAddConstraint() {
		return base.supportsAlterTableAddConstraint();
	}

	@Override
	public String getSQLToCheckTableExists(DBRow table) {
		return base.getSQLToCheckTableExists(table);
	}

	@Override
	public boolean supportsTableCheckingViaMetaData() {
		return base.supportsTableCheckingViaMetaData();
	}

	@Override
	public boolean requiresOnClauseForAllJoins() {
		return base.requiresOnClauseForAllJoins();
	}

	@Override
	public boolean requiresSequenceUpdateAfterManualInsert() {
		return base.requiresSequenceUpdateAfterManualInsert();
	}

	@Override
	public String getSequenceUpdateSQL(String tableName, String columnName, long primaryKeyGenerated) {
		return base.getSequenceUpdateSQL(tableName, columnName, primaryKeyGenerated);
	}

	@Override
	public Collection<? extends String> getInsertPreparation(DBRow table) {
		return base.getInsertPreparation(table);
	}

	@Override
	public Collection<? extends String> getInsertCleanUp(DBRow table) {
		return base.getInsertCleanUp(table);
	}

	@Override
	public String getAlterTableAddColumnSQL(DBRow existingTable, PropertyWrapper<?, ?, ?> columnPropertyWrapper) {
		return base.getAlterTableAddColumnSQL(existingTable, columnPropertyWrapper);
	}

	@Override
	public String getAddColumnColumnSQL(PropertyWrapper<?, ?, ?> field) {
		return base.getAddColumnColumnSQL(field);
	}

	@Override
	public boolean supportsNullsOrderingStandard() {
		return base.supportsNullsOrderingStandard();
	}

	@Override
	public String getNullsLast() {
		return base.getNullsLast();
	}

	@Override
	public String getNullsFirst() {
		return base.getNullsFirst();
	}

	@Override
	public String getNullsAnyOrder() {
		return base.getNullsAnyOrder();
	}

	@Override
	public String getTableExistsSQL(DBRow table) {
		return base.getTableExistsSQL(table);
	}

	@Override
	public boolean supportsDropTableIfExists() {
		return base.supportsDropTableIfExists();
	}

	@Override
	public String getDropTableIfExistsClause() {
		return base.getDropTableIfExistsClause();
	}

	@Override
	public String doStringAccumulateTransform(String accumulateColumn, String separator, String referencedTable) {
		return base.doStringAccumulateTransform(accumulateColumn, separator, referencedTable);
	}

	@Override
	public String doStringAccumulateTransform(String accumulateColumn, String separator, String orderByColumnName, String referencedTable) {
		return base.doStringAccumulateTransform(accumulateColumn, separator, orderByColumnName, referencedTable);
	}

	@Override
	public boolean requiresSortedSubselectForStringAggregate() {
		return base.requiresSortedSubselectForStringAggregate();
	}

	@Override
	public String doStringAccumulateTransform(StringExpression columnToAccumulate, StringExpression separator, SortProvider orderBy) {
		return base.doStringAccumulateTransform(columnToAccumulate, separator, orderBy);
	}

	@Override
	public boolean requiresClosedPolygons() {
		return base.requiresClosedPolygons();
	}

	@Override
	public boolean requiresReversingLineStringsFromDatabase() {
		return base.requiresReversingLineStringsFromDatabase();
	}

	@Override
	public DBExpression transformToSelectableType(DBExpression expression) {
		return base.transformToSelectableType(expression);
	}

	@Override
	public DBExpression transformToGroupableType(DBExpression expression) {
		return base.transformToGroupableType(expression);
	}

	@Override
	public boolean supportsBulkInserts() {
		return base.supportsBulkInserts();
	}

	@Override
	public boolean supportsWindowingFunctionsInTheHavingClause() {
		return base.supportsWindowingFunctionsInTheHavingClause();
	}

	@Override
	public boolean supportsWindowingFunctionsInTheOrderByClause() {
		return base.supportsWindowingFunctionsInTheOrderByClause();
	}

	@Override
	public String getRowNumberFunctionName() {
		return base.getRowNumberFunctionName();
	}

	@Override
	public String getDenseRankFunctionName() {
		return base.getDenseRankFunctionName();
	}

	@Override
	public String getRankFunctionName() {
		return base.getRankFunctionName();
	}

	@Override
	public String getNTilesFunctionName() {
		return base.getNTilesFunctionName();
	}

	@Override
	public String getPercentRankFunctionName() {
		return base.getPercentRankFunctionName();
	}

	@Override
	public String getFirstValueFunctionName() {
		return base.getFirstValueFunctionName();
	}

	@Override
	public String getLastValueFunctionName() {
		return base.getLastValueFunctionName();
	}

	@Override
	public String getNthValueFunctionName() {
		return base.getNthValueFunctionName();
	}

	@Override
	public String doNewLocalDateFromYearMonthDayTransform(String years, String months, String days) {
		return base.doNewLocalDateFromYearMonthDayTransform(years, months, days);
	}

	@Override
	public String doLeftPadTransform(String toPad, String padWith, String length) {
		return base.doLeftPadTransform(toPad, padWith, length);
	}

	@Override
	public boolean supportsLeftPadTransform() {
		return base.supportsLeftPadTransform();
	}

	@Override
	public String doRightPadTransform(String toPad, String padWith, String length) {
		return base.doRightPadTransform(toPad, padWith, length);
	}

	@Override
	public boolean supportsRightPadTransform() {
		return base.supportsRightPadTransform();
	}

	@Override
	public String doCurrentUTCDateTimeTransform() {
		return base.doCurrentUTCDateTimeTransform();
	}

	@Override
	public boolean supportsTimeZones() {
		return base.supportsTimeZones();
	}

	@Override
	public String getLagFunctionName() {
		return base.getLagFunctionName();
	}

	@Override
	public String getLeadFunctionName() {
		return base.getLeadFunctionName();
	}

	@Override
	public DBExpression transformToWhenableType(BooleanExpression test) {
		return base.transformToWhenableType(test);
	}

	@Override
	public String getDefaultOrderingClause() {
		return base.getDefaultOrderingClause();
	}

	@Override
	public String transformJavaDurationIntoDatabaseDuration(Duration interval) {
		return base.transformJavaDurationIntoDatabaseDuration(interval);
	}

	@Override
	public Duration parseDurationFromGetString(String intervalStr) {
		return base.parseDurationFromGetString(intervalStr);
	}

	@Override
	public String doDurationLessThanTransform(String toSQLString, String toSQLString0) {
		return base.doDurationLessThanTransform(toSQLString, toSQLString0);
	}

	@Override
	public String doDurationGreaterThanTransform(String toSQLString, String toSQLString0) {
		return base.doDurationGreaterThanTransform(toSQLString, toSQLString0);
	}

	@Override
	public String doDurationLessThanEqualsTransform(String toSQLString, String toSQLString0) {
		return base.doDurationLessThanEqualsTransform(toSQLString, toSQLString0);
	}

	@Override
	public String doDurationGreaterThanEqualsTransform(String toSQLString, String toSQLString0) {
		return base.doDurationGreaterThanEqualsTransform(toSQLString, toSQLString0);
	}

	@Override
	public String doDurationEqualsTransform(String toSQLString, String toSQLString0) {
		return base.doDurationEqualsTransform(toSQLString, toSQLString0);
	}

	@Override
	public String doDatePlusDurationTransform(String toSQLString, String toSQLString0) {
		return base.doDatePlusDurationTransform(toSQLString, toSQLString0);
	}

	@Override
	public String doDateMinusDurationTransform(String toSQLString, String toSQLString0) {
		return base.doDateMinusDurationTransform(toSQLString, toSQLString0);
	}

	@Override
	public boolean supportsDurationNatively() {
		return base.supportsDurationNatively();
	}

	@Override
	public int getParseDurationPartOffset() {
		return base.getParseDurationPartOffset();
	}

	@Override
	public String convertNullToEmptyString(String toSQLString) {
		return base.convertNullToEmptyString(toSQLString);
	}

	@Override
	public String doIsNullTransform(String expressionSQL) {
		return base.doIsNullTransform(expressionSQL);
	}

	@Override
	public DBDefinition getOracleCompatibleVersion() {
		return base.getOracleCompatibleVersion();
	}

	@Override
	public boolean hasLocalDateTimeOffset() {
		return base.hasLocalDateTimeOffset();
	}

	@Override
	public int getLocalDateTimeOffsetHours() {
		return base.getLocalDateTimeOffsetHours();
	}

	@Override
	public int getLocalDateTimeOffsetMinutes() {
		return base.getLocalDateTimeOffsetMinutes();
	}

	@Override
	public void setLocalDateTimeOffsetHours(int localDateTimeOffsetHours) {
		base.setLocalDateTimeOffsetHours(localDateTimeOffsetHours);
	}

	@Override
	public void setLocalDateTimeOffsetMinutes(int localDateTimeOffsetMinutes) {
		base.setLocalDateTimeOffsetMinutes(localDateTimeOffsetMinutes);
	}

	@Override
	public boolean requiresAddingTimeZoneToCurrentLocalDateTime() {
		return base.requiresAddingTimeZoneToCurrentLocalDateTime();
	}

	@Override
	public boolean supportsDateRepeatDatatypeFunctions() {
		return base.supportsDateRepeatDatatypeFunctions();
	}

}
