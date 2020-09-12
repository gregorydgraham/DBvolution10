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
package nz.co.gregs.dbvolution.databases;

import com.vividsolutions.jts.geom.LineSegment;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
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
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBLargeObject;
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
class ClusteredDefinition extends DBDefinition{

	private static final long serialVersionUID = 1L;
	private final DBDefinition internalDefinition;
	
	public ClusteredDefinition(DBDefinition defn){
		internalDefinition =defn;
	}

	@Override
	public int getNumericPrecision() {
		return internalDefinition.getNumericPrecision();
	}

	@Override
	public int getNumericScale() {
		return internalDefinition.getNumericScale();
	}

	@Override
	public String getDateFormattedForQuery(Date date) {
		return internalDefinition.getDateFormattedForQuery(date);
	}

	@Override
	public String getLocalDateTimeFormattedForQuery(LocalDateTime date) {
		return internalDefinition.getLocalDateTimeFormattedForQuery(date);
	}

	@Override
	public String getInstantFormattedForQuery(Instant instant) {
		return internalDefinition.getInstantFormattedForQuery(instant);
	}

	@Override
	public String getLocalDatePartsFormattedForQuery(String years, String months, String days, String hours, String minutes, String seconds, String subsecond, String timeZoneSign, String timeZoneHourOffset, String timeZoneMinuteOffSet) {
		return internalDefinition.getLocalDatePartsFormattedForQuery(years, months, days, hours, minutes, seconds, subsecond, timeZoneSign, timeZoneHourOffset, timeZoneMinuteOffSet);
	}

	@Override
	public String getInstantPartsFormattedForQuery(String years, String months, String days, String hours, String minutes, String seconds, String subsecond, String timeZoneSign, String timeZoneHourOffset, String timeZoneMinuteOffSet) {
		return internalDefinition.getInstantPartsFormattedForQuery(years, months, days, hours, minutes, seconds, subsecond, timeZoneSign, timeZoneHourOffset, timeZoneMinuteOffSet);
	}

	@Override
	public String getDatePartsFormattedForQuery(String years, String months, String days, String hours, String minutes, String seconds, String subsecond, String timeZoneSign, String timeZoneHourOffset, String timeZoneMinuteOffSet) {
		return internalDefinition.getDatePartsFormattedForQuery(years, months, days, hours, minutes, seconds, subsecond, timeZoneSign, timeZoneHourOffset, timeZoneMinuteOffSet);
	}

	@Override
	public String getUTCDateFormattedForQuery(Date date) {
		return internalDefinition.getUTCDateFormattedForQuery(date);
	}

	@Override
	public String formatColumnName(String columnName) {
		return internalDefinition.formatColumnName(columnName);
	}

	@Override
	public String beginStringValue() {
		return internalDefinition.beginStringValue();
	}

	@Override
	public String endStringValue() {
		return internalDefinition.endStringValue();
	}

	@Override
	public String beginNumberValue() {
		return internalDefinition.beginNumberValue();
	}

	@Override
	public String endNumberValue() {
		return internalDefinition.endNumberValue();
	}

	@Override
	public String formatTableAndColumnName(DBRow table, String columnName) {
		return internalDefinition.formatTableAndColumnName(table, columnName);
	}

	@Override
	public String formatTableAliasAndColumnName(RowDefinition table, String columnName) {
		return internalDefinition.formatTableAliasAndColumnName(table, columnName);
	}

	@Override
	public String formatTableAliasAndColumnNameForSelectClause(DBRow table, String columnName) {
		return internalDefinition.formatTableAliasAndColumnNameForSelectClause(table, columnName);
	}

	@Override
	public String formatTableName(DBRow table) {
		return internalDefinition.formatTableName(table);
	}

	@Override
	public String formatColumnNameForDBQueryResultSet(RowDefinition table, String columnName) {
		return internalDefinition.formatColumnNameForDBQueryResultSet(table, columnName);
	}

	@Override
	public String formatForColumnAlias(String actualName) {
		return internalDefinition.formatForColumnAlias(actualName);
	}

	@Override
	public String getTableAliasForObject(Object anObject) {
		return internalDefinition.getTableAliasForObject(anObject);
	}

	@Override
	public String formatExpressionAlias(Object key) {
		return internalDefinition.formatExpressionAlias(key);
	}

	@Override
	public String safeString(String toString) {
		return internalDefinition.safeString(toString);
	}

	@Override
	public String beginWhereClauseLine() {
		return internalDefinition.beginWhereClauseLine();
	}

	@Override
	public String beginConditionClauseLine(QueryOptions options) {
		return internalDefinition.beginConditionClauseLine(options);
	}

	@Override
	public String beginJoinClauseLine(QueryOptions options) {
		return internalDefinition.beginJoinClauseLine(options);
	}

	@Override
	public boolean prefersIndexBasedGroupByClause() {
		return internalDefinition.prefersIndexBasedGroupByClause();
	}

	@Override
	public String beginAndLine() {
		return internalDefinition.beginAndLine();
	}

	@Override
	public String beginOrLine() {
		return internalDefinition.beginOrLine();
	}

	@Override
	public String getDropTableStart() {
		return internalDefinition.getDropTableStart();
	}

	@Override
	public String getCreateTablePrimaryKeyClauseStart() {
		return internalDefinition.getCreateTablePrimaryKeyClauseStart();
	}

	@Override
	public String getCreateTablePrimaryKeyClauseMiddle() {
		return internalDefinition.getCreateTablePrimaryKeyClauseMiddle();
	}

	@Override
	public String getCreateTablePrimaryKeyClauseEnd() {
		return internalDefinition.getCreateTablePrimaryKeyClauseEnd();
	}

	@Override
	public String getCreateTableStart() {
		return internalDefinition.getCreateTableStart();
	}

	@Override
	public String getCreateTableColumnsStart() {
		return internalDefinition.getCreateTableColumnsStart();
	}

	@Override
	public String getCreateTableColumnsSeparator() {
		return internalDefinition.getCreateTableColumnsSeparator();
	}

	@Override
	public String getCreateTableColumnsNameAndTypeSeparator() {
		return internalDefinition.getCreateTableColumnsNameAndTypeSeparator();
	}

	@Override
	public Object getCreateTableColumnsEnd() {
		return internalDefinition.getCreateTableColumnsEnd();
	}

	@Override
	public String toLowerCase(String sql) {
		return internalDefinition.toLowerCase(sql);
	}

	@Override
	public String beginInsertLine() {
		return internalDefinition.beginInsertLine();
	}

	@Override
	public String endInsertLine() {
		return internalDefinition.endInsertLine();
	}

	@Override
	public String beginInsertColumnList() {
		return internalDefinition.beginInsertColumnList();
	}

	@Override
	public String endInsertColumnList() {
		return internalDefinition.endInsertColumnList();
	}

	@Override
	public String beginDeleteLine() {
		return internalDefinition.beginDeleteLine();
	}

	@Override
	public String endDeleteLine() {
		return internalDefinition.endDeleteLine();
	}

	@Override
	public String getEqualsComparator() {
		return internalDefinition.getEqualsComparator();
	}

	@Override
	public String getNotEqualsComparator() {
		return internalDefinition.getNotEqualsComparator();
	}

	@Override
	public String beginWhereClause() {
		return internalDefinition.beginWhereClause();
	}

	@Override
	public String beginUpdateLine() {
		return internalDefinition.beginUpdateLine();
	}

	@Override
	public String beginSetClause() {
		return internalDefinition.beginSetClause();
	}

	@Override
	public String getStartingSetSubClauseSeparator() {
		return internalDefinition.getStartingSetSubClauseSeparator();
	}

	@Override
	public String getSubsequentSetSubClauseSeparator() {
		return internalDefinition.getSubsequentSetSubClauseSeparator();
	}

	@Override
	public String getStartingOrderByClauseSeparator() {
		return internalDefinition.getStartingOrderByClauseSeparator();
	}

	@Override
	public String getSubsequentOrderByClauseSeparator() {
		return internalDefinition.getSubsequentOrderByClauseSeparator();
	}

	@Override
	public String getWhereClauseBeginningCondition() {
		return internalDefinition.getWhereClauseBeginningCondition();
	}

	@Override
	public String getWhereClauseBeginningCondition(QueryOptions options) {
		return internalDefinition.getWhereClauseBeginningCondition(options);
	}

	@Override
	public String getFalseOperation() {
		return internalDefinition.getFalseOperation();
	}

	@Override
	public String getTrueOperation() {
		return internalDefinition.getTrueOperation();
	}

	@Override
	public String getNull() {
		return internalDefinition.getNull();
	}

	@Override
	public String beginSelectStatement() {
		return internalDefinition.beginSelectStatement();
	}

	@Override
	public String beginFromClause() {
		return internalDefinition.beginFromClause();
	}

	@Override
	public String endSQLStatement() {
		return internalDefinition.endSQLStatement();
	}

	@Override
	public String getStartingSelectSubClauseSeparator() {
		return internalDefinition.getStartingSelectSubClauseSeparator();
	}

	@Override
	public String getSubsequentSelectSubClauseSeparator() {
		return internalDefinition.getSubsequentSelectSubClauseSeparator();
	}

	@Override
	public String countStarClause() {
		return internalDefinition.countStarClause();
	}

	@Override
	public String getLimitRowsSubClauseDuringSelectClause(QueryOptions options) {
		return internalDefinition.getLimitRowsSubClauseDuringSelectClause(options);
	}

	@Override
	public String beginOrderByClause() {
		return internalDefinition.beginOrderByClause();
	}

	@Override
	public String endOrderByClause() {
		return internalDefinition.endOrderByClause();
	}

	@Override
	public String getOrderByDirectionClause(Boolean sortOrder) {
		return internalDefinition.getOrderByDirectionClause(sortOrder);
	}

	@Override
	public String getOrderByDirectionClause(SortProvider.Ordering sortOrder) {
		return internalDefinition.getOrderByDirectionClause(sortOrder);
	}

	@Override
	public String getOrderByDescending() {
		return internalDefinition.getOrderByDescending();
	}

	@Override
	public String getOrderByAscending() {
		return internalDefinition.getOrderByAscending();
	}

	@Override
	public String beginInnerJoin() {
		return internalDefinition.beginInnerJoin();
	}

	@Override
	public String beginLeftOuterJoin() {
		return internalDefinition.beginLeftOuterJoin();
	}

	@Override
	public String beginRightOuterJoin() {
		return internalDefinition.beginRightOuterJoin();
	}

	@Override
	public String beginFullOuterJoin() {
		return internalDefinition.beginFullOuterJoin();
	}

	@Override
	public String beginOnClause() {
		return internalDefinition.beginOnClause();
	}

	@Override
	public String endOnClause() {
		return internalDefinition.endOnClause();
	}

	@Override
	public String getLimitRowsSubClauseAfterWhereClause(QueryState state, QueryOptions options) {
		return internalDefinition.getLimitRowsSubClauseAfterWhereClause(state, options);
	}

	@Override
	public String getPreparedVariableSymbol() {
		return internalDefinition.getPreparedVariableSymbol();
	}

	@Override
	public boolean isColumnNamesCaseSensitive() {
		return internalDefinition.isColumnNamesCaseSensitive();
	}

	@Override
	public String startMultilineComment() {
		return internalDefinition.startMultilineComment();
	}

	@Override
	public String endMultilineComment() {
		return internalDefinition.endMultilineComment();
	}

	@Override
	public String beginValueClause() {
		return internalDefinition.beginValueClause();
	}

	@Override
	public String beginValueSeparatorClause() {
		return internalDefinition.beginValueSeparatorClause();
	}

	@Override
	public Object endValueClause() {
		return internalDefinition.endValueClause();
	}

	@Override
	public String getValuesClauseValueSeparator() {
		return internalDefinition.getValuesClauseValueSeparator();
	}

	@Override
	public String getValuesClauseColumnSeparator() {
		return internalDefinition.getValuesClauseColumnSeparator();
	}

	@Override
	public String beginTableAlias() {
		return internalDefinition.beginTableAlias();
	}

	@Override
	public String endTableAlias() {
		return internalDefinition.endTableAlias();
	}

	@Override
	public String getTableAlias(RowDefinition tabRow) {
		return internalDefinition.getTableAlias(tabRow);
	}

	@Override
	public String formatTableAlias(String suggestedTableAlias) {
		return internalDefinition.formatTableAlias(suggestedTableAlias);
	}

	@Override
	public String doCurrentDateTimeTransform() {
		return internalDefinition.doCurrentDateTimeTransform();
	}

	@Override
	public String getDefaultTimeZoneSign() {
		return internalDefinition.getDefaultTimeZoneSign();
	}

	@Override
	public String getDefaultTimeZoneHour() {
		return internalDefinition.getDefaultTimeZoneHour();
	}

	@Override
	public String getDefaultTimeZoneMinute() {
		return internalDefinition.getDefaultTimeZoneMinute();
	}

	@Override
	public String doCurrentTimeTransform() {
		return internalDefinition.doCurrentTimeTransform();
	}

	@Override
	public String doCurrentUTCTimeTransform() {
		return internalDefinition.doCurrentUTCTimeTransform();
	}

	@Override
	public String getDropDatabase(String databaseName) throws UnsupportedOperationException {
		return internalDefinition.getDropDatabase(databaseName);
	}

	@Override
	public String doLeftTrimTransform(String enclosedValue) {
		return internalDefinition.doLeftTrimTransform(enclosedValue);
	}

	@Override
	public String doLowercaseTransform(String enclosedValue) {
		return internalDefinition.doLowercaseTransform(enclosedValue);
	}

	@Override
	public String doRightTrimTransform(String enclosedValue) {
		return internalDefinition.doRightTrimTransform(enclosedValue);
	}

	@Override
	public String doStringLengthTransform(String enclosedValue) {
		return internalDefinition.doStringLengthTransform(enclosedValue);
	}

	@Override
	public String doTrimFunction(String enclosedValue) {
		return internalDefinition.doTrimFunction(enclosedValue);
	}

	@Override
	public String doUppercaseTransform(String enclosedValue) {
		return internalDefinition.doUppercaseTransform(enclosedValue);
	}

	@Override
	public String doConcatTransform(String firstString, String secondString) {
		return internalDefinition.doConcatTransform(firstString, secondString);
	}

	@Override
	public String doConcatTransform(String firstString, String secondString, String... rest) {
		return internalDefinition.doConcatTransform(firstString, secondString, rest);
	}

	@Override
	public String getNextSequenceValueFunctionName() {
		return internalDefinition.getNextSequenceValueFunctionName();
	}

	@Override
	public String getRightTrimFunctionName() {
		return internalDefinition.getRightTrimFunctionName();
	}

	@Override
	public String getLowercaseFunctionName() {
		return internalDefinition.getLowercaseFunctionName();
	}

	@Override
	public String getUppercaseFunctionName() {
		return internalDefinition.getUppercaseFunctionName();
	}

	@Override
	public String getStringLengthFunctionName() {
		return internalDefinition.getStringLengthFunctionName();
	}

	@Override
	public String getCurrentUserFunctionName() {
		return internalDefinition.getCurrentUserFunctionName();
	}

	@Override
	public String doYearTransform(String dateExpression) {
		return internalDefinition.doYearTransform(dateExpression);
	}

	@Override
	public String doMonthTransform(String dateExpression) {
		return internalDefinition.doMonthTransform(dateExpression);
	}

	@Override
	public String doDayTransform(String dateExpression) {
		return internalDefinition.doDayTransform(dateExpression);
	}

	@Override
	public String doHourTransform(String dateExpression) {
		return internalDefinition.doHourTransform(dateExpression);
	}

	@Override
	public String doMinuteTransform(String dateExpression) {
		return internalDefinition.doMinuteTransform(dateExpression);
	}

	@Override
	public String doSecondTransform(String dateExpression) {
		return internalDefinition.doSecondTransform(dateExpression);
	}

	@Override
	public String doSubsecondTransform(String dateExpression) {
		return internalDefinition.doSubsecondTransform(dateExpression);
	}

	@Override
	public String doComparableInstantTransform(String instantExpression) {
		return internalDefinition.doComparableInstantTransform(instantExpression);
	}

	@Override
	public String doInstantYearTransform(String dateExpression) {
		return internalDefinition.doInstantYearTransform(dateExpression);
	}

	@Override
	public String doInstantMonthTransform(String dateExpression) {
		return internalDefinition.doInstantMonthTransform(dateExpression);
	}

	@Override
	public String doInstantDayTransform(String dateExpression) {
		return internalDefinition.doInstantDayTransform(dateExpression);
	}

	@Override
	public String doInstantHourTransform(String dateExpression) {
		return internalDefinition.doInstantHourTransform(dateExpression);
	}

	@Override
	public String doInstantMinuteTransform(String dateExpression) {
		return internalDefinition.doInstantMinuteTransform(dateExpression);
	}

	@Override
	public String doInstantSecondTransform(String dateExpression) {
		return internalDefinition.doInstantSecondTransform(dateExpression);
	}

	@Override
	public String doInstantSubsecondTransform(String dateExpression) {
		return internalDefinition.doInstantSubsecondTransform(dateExpression);
	}

	@Override
	public String doPositionInStringTransform(String originalString, String stringToFind) {
		return internalDefinition.doPositionInStringTransform(originalString, stringToFind);
	}

	@Override
	public String getIfNullFunctionName() {
		return internalDefinition.getIfNullFunctionName();
	}

	@Override
	public boolean supportsComparingBooleanResults() {
		return internalDefinition.supportsComparingBooleanResults();
	}

	@Override
	public String getNegationFunctionName() {
		return internalDefinition.getNegationFunctionName();
	}

	@Override
	public String getSubsequentGroupBySubClauseSeparator() {
		return internalDefinition.getSubsequentGroupBySubClauseSeparator();
	}

	@Override
	public String beginGroupByClause() {
		return internalDefinition.beginGroupByClause();
	}

	@Override
	public String getAverageFunctionName() {
		return internalDefinition.getAverageFunctionName();
	}

	@Override
	public String getCountFunctionName() {
		return internalDefinition.getCountFunctionName();
	}

	@Override
	public String getMaxFunctionName() {
		return internalDefinition.getMaxFunctionName();
	}

	@Override
	public String getMinFunctionName() {
		return internalDefinition.getMinFunctionName();
	}

	@Override
	public String getSumFunctionName() {
		return internalDefinition.getSumFunctionName();
	}

	@Override
	public String getStandardDeviationFunctionName() {
		return internalDefinition.getStandardDeviationFunctionName();
	}

	@Override
	public boolean prefersIndexBasedOrderByClause() {
		return internalDefinition.prefersIndexBasedOrderByClause();
	}

	@Override
	public boolean supportsPagingNatively(QueryOptions options) {
		return internalDefinition.supportsPagingNatively(options);
	}

	@Override
	public boolean supportsGeneratedKeys() {
		return internalDefinition.supportsGeneratedKeys();
	}

	@Override
	public String getTruncFunctionName() {
		return internalDefinition.getTruncFunctionName();
	}

	@Override
	public String doTruncTransform(String realNumberExpression, String numberOfDecimalPlacesExpression) {
		return internalDefinition.doTruncTransform(realNumberExpression, numberOfDecimalPlacesExpression);
	}

	@Override
	public String doStringEqualsTransform(String firstSQLExpression, String secondSQLExpression) {
		return internalDefinition.doStringEqualsTransform(firstSQLExpression, secondSQLExpression);
	}

	@Override
	public String doBooleanToIntegerTransform(String booleanExpression) {
		return internalDefinition.doBooleanToIntegerTransform(booleanExpression);
	}

	@Override
	public String doIntegerToBitTransform(String bitExpression) {
		return internalDefinition.doIntegerToBitTransform(bitExpression);
	}

	@Override
	public String getColumnAutoIncrementSuffix() {
		return internalDefinition.getColumnAutoIncrementSuffix();
	}

	@Override
	public boolean prefersTriggerBasedIdentities() {
		return internalDefinition.prefersTriggerBasedIdentities();
	}

	@Override
	public List<String> getTriggerBasedIdentitySQL(DBDatabase db, String table, String column) {
		return internalDefinition.getTriggerBasedIdentitySQL(db, table, column);
	}

	@Override
	public List<String> dropTriggerBasedIdentitySQL(DBDatabase db, String table, String column) {
		return internalDefinition.dropTriggerBasedIdentitySQL(db, table, column);
	}

	@Override
	public String getPrimaryKeySequenceName(String table, String column) {
		return internalDefinition.getPrimaryKeySequenceName(table, column);
	}

	@Override
	public String getPrimaryKeyTriggerName(String table, String column) {
		return internalDefinition.getPrimaryKeyTriggerName(table, column);
	}

	@Override
	public boolean prefersTrailingPrimaryKeyDefinition() {
		return internalDefinition.prefersTrailingPrimaryKeyDefinition();
	}

	@Override
	public boolean prefersLargeObjectsReadAsBase64CharacterStream(DBLargeObject<?> lob) {
		return internalDefinition.prefersLargeObjectsReadAsBase64CharacterStream(lob);
	}

	@Override
	public boolean prefersLargeObjectsReadAsBytes(DBLargeObject<?> lob) {
		return internalDefinition.prefersLargeObjectsReadAsBytes(lob);
	}

	@Override
	public boolean prefersLargeObjectsReadAsCLOB(DBLargeObject<?> lob) {
		return internalDefinition.prefersLargeObjectsReadAsCLOB(lob);
	}

	@Override
	public boolean prefersLargeObjectsReadAsBLOB(DBLargeObject<?> lob) {
		return internalDefinition.prefersLargeObjectsReadAsBLOB(lob);
	}

	@Override
	public String doSubstringTransform(String originalString, String start, String length) {
		return internalDefinition.doSubstringTransform(originalString, start, length);
	}

	@Override
	public boolean prefersLargeObjectsSetAsCharacterStream(DBLargeObject<?> lob) {
		return internalDefinition.prefersLargeObjectsSetAsCharacterStream(lob);
	}

	@Override
	public boolean prefersLargeObjectsSetAsBLOB(DBLargeObject<?> lob) {
		return internalDefinition.prefersLargeObjectsSetAsBLOB(lob);
	}

	@Override
	public boolean prefersLargeObjectsSetAsBase64String(DBLargeObject<?> lob) {
		return internalDefinition.prefersLargeObjectsSetAsBase64String(lob);
	}

	@Override
	public String getGreatestOfFunctionName() {
		return internalDefinition.getGreatestOfFunctionName();
	}

	@Override
	public String getLeastOfFunctionName() {
		return internalDefinition.getLeastOfFunctionName();
	}

	@Override
	public String getCheezBurger() {
		return internalDefinition.getCheezBurger();
	}

	@Override
	public boolean prefersDatesReadAsStrings() {
		return internalDefinition.prefersDatesReadAsStrings();
	}

	@Override
	public boolean prefersInstantsReadAsStrings() {
		return internalDefinition.prefersInstantsReadAsStrings();
	}

	@Override
	public Date parseDateFromGetString(String getStringDate) throws ParseException {
		return internalDefinition.parseDateFromGetString(getStringDate);
	}

	@Override
	public LocalDate parseLocalDateFromGetString(String getStringDate) throws ParseException {
		return internalDefinition.parseLocalDateFromGetString(getStringDate);
	}

	@Override
	public LocalDateTime parseLocalDateTimeFromGetString(String inputFromResultSet) throws ParseException {
		return internalDefinition.parseLocalDateTimeFromGetString(inputFromResultSet);
	}

	@Override
	public Instant parseInstantFromGetString(String inputFromResultSet) throws ParseException {
		return internalDefinition.parseInstantFromGetString(inputFromResultSet);
	}

	@Override
	public void sanityCheckDBTableField(DBTableField dbTableField) {
		internalDefinition.sanityCheckDBTableField(dbTableField);
	}

	@Override
	public boolean supportsRetrievingLastInsertedRowViaSQL() {
		return internalDefinition.supportsRetrievingLastInsertedRowViaSQL();
	}

	@Override
	public String getRetrieveLastInsertedRowSQL() {
		return internalDefinition.getRetrieveLastInsertedRowSQL();
	}

	@Override
	public String getEmptyString() {
		return internalDefinition.getEmptyString();
	}

	@Override
	public boolean supportsDegreesFunction() {
		return internalDefinition.supportsDegreesFunction();
	}

	@Override
	public boolean supportsRadiansFunction() {
		return internalDefinition.supportsRadiansFunction();
	}

	@Override
	public String doRadiansTransform(String degreesSQL) {
		return internalDefinition.doRadiansTransform(degreesSQL);
	}

	@Override
	public String doDegreesTransform(String radiansSQL) {
		return internalDefinition.doDegreesTransform(radiansSQL);
	}

	@Override
	public String getExpFunctionName() {
		return internalDefinition.getExpFunctionName();
	}

	@Override
	public boolean supportsExpFunction() {
		return internalDefinition.supportsExpFunction();
	}

	@Override
	public boolean supportsStandardDeviationFunction() {
		return internalDefinition.supportsStandardDeviationFunction();
	}

	@Override
	public boolean supportsModulusFunction() {
		return internalDefinition.supportsModulusFunction();
	}

	@Override
	public String doModulusTransform(String firstNumber, String secondNumber) {
		return internalDefinition.doModulusTransform(firstNumber, secondNumber);
	}

	@Override
	public String doDateAddSecondsTransform(String dateValue, String numberOfSeconds) {
		return internalDefinition.doDateAddSecondsTransform(dateValue, numberOfSeconds);
	}

	@Override
	public String doDateAddMinutesTransform(String dateValue, String numberOfMinutes) {
		return internalDefinition.doDateAddMinutesTransform(dateValue, numberOfMinutes);
	}

	@Override
	public String doDateAddDaysTransform(String dateValue, String numberOfDays) {
		return internalDefinition.doDateAddDaysTransform(dateValue, numberOfDays);
	}

	@Override
	public String doDateAddHoursTransform(String dateValue, String numberOfHours) {
		return internalDefinition.doDateAddHoursTransform(dateValue, numberOfHours);
	}

	@Override
	public String doDateAddWeeksTransform(String dateValue, String numberOfWeeks) {
		return internalDefinition.doDateAddWeeksTransform(dateValue, numberOfWeeks);
	}

	@Override
	public String doDateAddMonthsTransform(String dateValue, String numberOfMonths) {
		return internalDefinition.doDateAddMonthsTransform(dateValue, numberOfMonths);
	}

	@Override
	public String doDateAddYearsTransform(String dateValue, String numberOfYears) {
		return internalDefinition.doDateAddYearsTransform(dateValue, numberOfYears);
	}

	@Override
	public String doInstantAddSecondsTransform(String InstantValue, String numberOfSeconds) {
		return internalDefinition.doInstantAddSecondsTransform(InstantValue, numberOfSeconds);
	}

	@Override
	public String doInstantAddMinutesTransform(String instantValue, String numberOfMinutes) {
		return internalDefinition.doInstantAddMinutesTransform(instantValue, numberOfMinutes);
	}

	@Override
	public String doInstantAddDaysTransform(String instantValue, String numberOfDays) {
		return internalDefinition.doInstantAddDaysTransform(instantValue, numberOfDays);
	}

	@Override
	public String doInstantAddHoursTransform(String instantValue, String numberOfHours) {
		return internalDefinition.doInstantAddHoursTransform(instantValue, numberOfHours);
	}

	@Override
	public String doInstantAddWeeksTransform(String instantValue, String numberOfWeeks) {
		return internalDefinition.doInstantAddWeeksTransform(instantValue, numberOfWeeks);
	}

	@Override
	public String doInstantAddMonthsTransform(String instantValue, String numberOfMonths) {
		return internalDefinition.doInstantAddMonthsTransform(instantValue, numberOfMonths);
	}

	@Override
	public String doInstantAddYearsTransform(String instantValue, String numberOfYears) {
		return internalDefinition.doInstantAddYearsTransform(instantValue, numberOfYears);
	}

	@Override
	public String doBooleanValueTransform(Boolean boolValue) {
		return internalDefinition.doBooleanValueTransform(boolValue);
	}

	@Override
	public boolean supportsXOROperator() {
		return internalDefinition.supportsXOROperator();
	}

	@Override
	public String doLeastOfTransformation(List<String> strs) {
		return internalDefinition.doLeastOfTransformation(strs);
	}

	@Override
	public String doGreatestOfTransformation(List<String> strs) {
		return internalDefinition.doGreatestOfTransformation(strs);
	}

	@Override
	public String doReplaceTransform(String withinString, String findString, String replaceString) {
		return internalDefinition.doReplaceTransform(withinString, findString, replaceString);
	}

	@Override
	public String doNumberToStringTransform(String numberExpression) {
		return internalDefinition.doNumberToStringTransform(numberExpression);
	}

	@Override
	public String doIntegerToStringTransform(String integerExpression) {
		return internalDefinition.doIntegerToStringTransform(integerExpression);
	}

	@Override
	public String doCurrentDateOnlyTransform() {
		return internalDefinition.doCurrentDateOnlyTransform();
	}

	@Override
	public String doBitsValueTransform(boolean[] booleanArray) {
		return internalDefinition.doBitsValueTransform(booleanArray);
	}

	@Override
	public String doDayDifferenceTransform(String dateValue, String otherDateValue) {
		return internalDefinition.doDayDifferenceTransform(dateValue, otherDateValue);
	}

	@Override
	public String doWeekDifferenceTransform(String dateValue, String otherDateValue) {
		return internalDefinition.doWeekDifferenceTransform(dateValue, otherDateValue);
	}

	@Override
	public String doMonthDifferenceTransform(String dateValue, String otherDateValue) {
		return internalDefinition.doMonthDifferenceTransform(dateValue, otherDateValue);
	}

	@Override
	public String doYearDifferenceTransform(String dateValue, String otherDateValue) {
		return internalDefinition.doYearDifferenceTransform(dateValue, otherDateValue);
	}

	@Override
	public String doHourDifferenceTransform(String dateValue, String otherDateValue) {
		return internalDefinition.doHourDifferenceTransform(dateValue, otherDateValue);
	}

	@Override
	public String doMinuteDifferenceTransform(String dateValue, String otherDateValue) {
		return internalDefinition.doMinuteDifferenceTransform(dateValue, otherDateValue);
	}

	@Override
	public String doSecondDifferenceTransform(String dateValue, String otherDateValue) {
		return internalDefinition.doSecondDifferenceTransform(dateValue, otherDateValue);
	}

	@Override
	public String getForeignKeyClauseForCreateTable(PropertyWrapper field) {
		return internalDefinition.getForeignKeyClauseForCreateTable(field);
	}

	@Override
	public String doStringIfNullTransform(String possiblyNullValue, String alternativeIfNull) {
		return internalDefinition.doStringIfNullTransform(possiblyNullValue, alternativeIfNull);
	}

	@Override
	public String doNumberIfNullTransform(String possiblyNullValue, String alternativeIfNull) {
		return internalDefinition.doNumberIfNullTransform(possiblyNullValue, alternativeIfNull);
	}

	@Override
	public String doIntegerIfNullTransform(String possiblyNullValue, String alternativeIfNull) {
		return internalDefinition.doIntegerIfNullTransform(possiblyNullValue, alternativeIfNull);
	}

	@Override
	public String doDateIfNullTransform(String possiblyNullValue, String alternativeIfNull) {
		return internalDefinition.doDateIfNullTransform(possiblyNullValue, alternativeIfNull);
	}

	@Override
	public String doInTransform(String comparableValue, List<String> values) {
		return internalDefinition.doInTransform(comparableValue, values);
	}

	@Override
	public String doNotInTransform(String comparableValue, List<String> values) {
		return internalDefinition.doNotInTransform(comparableValue, values);
	}

	@Override
	public String getFromClause(DBRow table) {
		return internalDefinition.getFromClause(table);
	}

	@Override
	public String beginWithClause() {
		return internalDefinition.beginWithClause();
	}

	@Override
	public String formatWithClauseTableDefinition(String recursiveTableAlias, String recursiveColumnNames) {
		return internalDefinition.formatWithClauseTableDefinition(recursiveTableAlias, recursiveColumnNames);
	}

	@Override
	public String beginWithClausePrimingQuery() {
		return internalDefinition.beginWithClausePrimingQuery();
	}

	@Override
	public String endWithClausePrimingQuery() {
		return internalDefinition.endWithClausePrimingQuery();
	}

	@Override
	public String beginWithClauseRecursiveQuery() {
		return internalDefinition.beginWithClauseRecursiveQuery();
	}

	@Override
	public String endWithClauseRecursiveQuery() {
		return internalDefinition.endWithClauseRecursiveQuery();
	}

	@Override
	public String doSelectFromRecursiveTable(String recursiveTableAlias, String recursiveAliases) {
		return internalDefinition.doSelectFromRecursiveTable(recursiveTableAlias, recursiveAliases);
	}

	@Override
	public boolean requiresRecursiveTableAlias() {
		return internalDefinition.requiresRecursiveTableAlias();
	}

	@Override
	public String getRecursiveQueryDepthColumnName() {
		return internalDefinition.getRecursiveQueryDepthColumnName();
	}

	@Override
	public boolean supportsPurelyFunctionalGroupByColumns() {
		return internalDefinition.supportsPurelyFunctionalGroupByColumns();
	}

	@Override
	public String getSystemTableExclusionPattern() {
		return internalDefinition.getSystemTableExclusionPattern();
	}

	@Override
	public String formatPrimaryKeyForRetrievingGeneratedKeys(String primaryKeyColumnName) {
		return internalDefinition.formatPrimaryKeyForRetrievingGeneratedKeys(primaryKeyColumnName);
	}

	@Override
	public String doChooseTransformation(String numberToChooseWith, List<String> strs) {
		return internalDefinition.doChooseTransformation(numberToChooseWith, strs);
	}

	@Override
	public String getChooseFunctionName() {
		return internalDefinition.getChooseFunctionName();
	}

	@Override
	public String doIfThenElseTransform(String booleanTest, String thenResult, String elseResult) {
		return internalDefinition.doIfThenElseTransform(booleanTest, thenResult, elseResult);
	}

	@Override
	public String doIfEmptyStringThenElse(String expressionSQL, String ifResult, String thenResult) {
		return internalDefinition.doIfEmptyStringThenElse(expressionSQL, ifResult, thenResult);
	}

	@Override
	public String doIfNullThenElse(String expressionSQL, String ifResult, String thenResult) {
		return internalDefinition.doIfNullThenElse(expressionSQL, ifResult, thenResult);
	}

	@Override
	public String doDayOfWeekTransform(String dateSQL) {
		return internalDefinition.doDayOfWeekTransform(dateSQL);
	}

	@Override
	public String doInstantDayOfWeekTransform(String dateSQL) {
		return internalDefinition.doInstantDayOfWeekTransform(dateSQL);
	}

	@Override
	public String getIndexClauseForCreateTable(PropertyWrapper field) {
		return internalDefinition.getIndexClauseForCreateTable(field);
	}

	@Override
	public String doBooleanArrayTransform(Boolean[] bools) {
		return internalDefinition.doBooleanArrayTransform(bools);
	}

	@Override
	public Boolean[] doBooleanArrayResultInterpretation(String stringOfBools) {
		return internalDefinition.doBooleanArrayResultInterpretation(stringOfBools);
	}

	@Override
	public boolean supportsArraysNatively() {
		return internalDefinition.supportsArraysNatively();
	}

	@Override
	public Boolean doBooleanArrayElementTransform(Object objRepresentingABoolean) {
		return internalDefinition.doBooleanArrayElementTransform(objRepresentingABoolean);
	}

	@Override
	public String doNumberEqualsTransform(String leftHandSide, String rightHandSide) {
		return internalDefinition.doNumberEqualsTransform(leftHandSide, rightHandSide);
	}

	@Override
	public String doIntegerEqualsTransform(String leftHandSide, String rightHandSide) {
		return internalDefinition.doIntegerEqualsTransform(leftHandSide, rightHandSide);
	}

	@Override
	public String getAlterTableAddForeignKeyStatement(DBRow newTableRow, PropertyWrapper field) {
		return internalDefinition.getAlterTableAddForeignKeyStatement(newTableRow, field);
	}

	@Override
	public String getAlterTableDropForeignKeyStatement(DBRow newTableRow, PropertyWrapper field) {
		return internalDefinition.getAlterTableDropForeignKeyStatement(newTableRow, field);
	}

	@Override
	public String transformPeriodIntoDateRepeat(Period interval) {
		return internalDefinition.transformPeriodIntoDateRepeat(interval);
	}

	@Override
	public String doDateMinusToDateRepeatTransformation(String leftHandSide, String rightHandSide) {
		return internalDefinition.doDateMinusToDateRepeatTransformation(leftHandSide, rightHandSide);
	}

	@Override
	public String doDateRepeatEqualsTransform(String leftHandSide, String rightHandSide) {
		return internalDefinition.doDateRepeatEqualsTransform(leftHandSide, rightHandSide);
	}

	@Override
	public String doDateRepeatNotEqualsTransform(String leftHandSide, String rightHandSide) {
		return internalDefinition.doDateRepeatNotEqualsTransform(leftHandSide, rightHandSide);
	}

	@Override
	public String doDateRepeatLessThanTransform(String leftHandSide, String rightHandSide) {
		return internalDefinition.doDateRepeatLessThanTransform(leftHandSide, rightHandSide);
	}

	@Override
	public String doDateRepeatLessThanEqualsTransform(String leftHandSide, String rightHandSide) {
		return internalDefinition.doDateRepeatLessThanEqualsTransform(leftHandSide, rightHandSide);
	}

	@Override
	public String doDateRepeatGreaterThanTransform(String leftHandSide, String rightHandSide) {
		return internalDefinition.doDateRepeatGreaterThanTransform(leftHandSide, rightHandSide);
	}

	@Override
	public String doDateRepeatGreaterThanEqualsTransform(String leftHandSide, String rightHandSide) {
		return internalDefinition.doDateRepeatGreaterThanEqualsTransform(leftHandSide, rightHandSide);
	}

	@Override
	public String doDatePlusDateRepeatTransform(String leftHandSide, String rightHandSide) {
		return internalDefinition.doDatePlusDateRepeatTransform(leftHandSide, rightHandSide);
	}

	@Override
	public String doDateMinusDateRepeatTransform(String leftHandSide, String rightHandSide) {
		return internalDefinition.doDateMinusDateRepeatTransform(leftHandSide, rightHandSide);
	}

	@Override
	public Period parseDateRepeatFromGetString(String intervalStr) {
		return internalDefinition.parseDateRepeatFromGetString(intervalStr);
	}

	@Override
	public String doPolygon2DEqualsTransform(String firstGeometry, String secondGeometry) {
		return internalDefinition.doPolygon2DEqualsTransform(firstGeometry, secondGeometry);
	}

	@Override
	public String doPolygon2DUnionTransform(String firstGeometry, String secondGeometry) {
		return internalDefinition.doPolygon2DUnionTransform(firstGeometry, secondGeometry);
	}

	@Override
	public String doPolygon2DIntersectionTransform(String firstGeometry, String secondGeometry) {
		return internalDefinition.doPolygon2DIntersectionTransform(firstGeometry, secondGeometry);
	}

	@Override
	public String doPolygon2DIntersectsTransform(String firstGeometry, String secondGeometry) {
		return internalDefinition.doPolygon2DIntersectsTransform(firstGeometry, secondGeometry);
	}

	@Override
	public String doPolygon2DContainsPolygon2DTransform(String firstGeometry, String secondGeometry) {
		return internalDefinition.doPolygon2DContainsPolygon2DTransform(firstGeometry, secondGeometry);
	}

	@Override
	public String doPolygon2DDoesNotIntersectTransform(String firstGeometry, String secondGeometry) {
		return internalDefinition.doPolygon2DDoesNotIntersectTransform(firstGeometry, secondGeometry);
	}

	@Override
	public String doPolygon2DOverlapsTransform(String firstGeometry, String secondGeometry) {
		return internalDefinition.doPolygon2DOverlapsTransform(firstGeometry, secondGeometry);
	}

	@Override
	public String doPolygon2DTouchesTransform(String firstGeometry, String secondGeometry) {
		return internalDefinition.doPolygon2DTouchesTransform(firstGeometry, secondGeometry);
	}

	@Override
	public String doPolygon2DWithinTransform(String firstGeometry, String secondGeometry) {
		return internalDefinition.doPolygon2DWithinTransform(firstGeometry, secondGeometry);
	}

	@Override
	public String doPolygon2DMeasurableDimensionsTransform(String polygon2DSQL) {
		return internalDefinition.doPolygon2DMeasurableDimensionsTransform(polygon2DSQL);
	}

	@Override
	public String doPolygon2DGetBoundingBoxTransform(String polygon2DSQL) {
		return internalDefinition.doPolygon2DGetBoundingBoxTransform(polygon2DSQL);
	}

	@Override
	public String doPolygon2DGetAreaTransform(String polygon2DSQL) {
		return internalDefinition.doPolygon2DGetAreaTransform(polygon2DSQL);
	}

	@Override
	public String doPolygon2DGetExteriorRingTransform(String polygon2DSQL) {
		return internalDefinition.doPolygon2DGetExteriorRingTransform(polygon2DSQL);
	}

	@Override
	public boolean supportsHyperbolicFunctionsNatively() {
		return internalDefinition.supportsHyperbolicFunctionsNatively();
	}

	@Override
	public String getArctan2FunctionName() {
		return internalDefinition.getArctan2FunctionName();
	}

	@Override
	public String doDateRepeatGetYearsTransform(String dateRepeatSQL) {
		return internalDefinition.doDateRepeatGetYearsTransform(dateRepeatSQL);
	}

	@Override
	public String doDateRepeatGetMonthsTransform(String dateRepeatSQL) {
		return internalDefinition.doDateRepeatGetMonthsTransform(dateRepeatSQL);
	}

	@Override
	public String doDateRepeatGetDaysTransform(String dateRepeatSQL) {
		return internalDefinition.doDateRepeatGetDaysTransform(dateRepeatSQL);
	}

	@Override
	public String doDateRepeatGetHoursTransform(String dateRepeatSQL) {
		return internalDefinition.doDateRepeatGetHoursTransform(dateRepeatSQL);
	}

	@Override
	public String doDateRepeatGetMinutesTransform(String dateRepeatSQL) {
		return internalDefinition.doDateRepeatGetMinutesTransform(dateRepeatSQL);
	}

	@Override
	public String doDateRepeatGetSecondsTransform(String dateRepeatSQL) {
		return internalDefinition.doDateRepeatGetSecondsTransform(dateRepeatSQL);
	}

	@Override
	public String doDateRepeatToStringTransform(String dateRepeatSQL) {
		return internalDefinition.doDateRepeatToStringTransform(dateRepeatSQL);
	}

	@Override
	public String doStringToNumberTransform(String stringResultContainingANumber) {
		return internalDefinition.doStringToNumberTransform(stringResultContainingANumber);
	}

	@Override
	public boolean supportsArcSineFunction() {
		return internalDefinition.supportsArcSineFunction();
	}

	@Override
	public boolean supportsCotangentFunction() {
		return internalDefinition.supportsCotangentFunction();
	}

	@Override
	public DBExpression transformToStorableType(DBExpression columnExpression) {
		return internalDefinition.transformToStorableType(columnExpression);
	}

	@Override
	public DBExpression transformToSortableType(DBExpression columnExpression) {
		return internalDefinition.transformToSortableType(columnExpression);
	}

	@Override
	public String doPoint2DEqualsTransform(String firstPoint, String secondPoint) {
		return internalDefinition.doPoint2DEqualsTransform(firstPoint, secondPoint);
	}

	@Override
	public String doPoint2DGetXTransform(String pont2DSQL) {
		return internalDefinition.doPoint2DGetXTransform(pont2DSQL);
	}

	@Override
	public String doPoint2DGetYTransform(String point2DSQL) {
		return internalDefinition.doPoint2DGetYTransform(point2DSQL);
	}

	@Override
	public String doPoint2DMeasurableDimensionsTransform(String point2DSQL) {
		return internalDefinition.doPoint2DMeasurableDimensionsTransform(point2DSQL);
	}

	@Override
	public String doPoint2DGetBoundingBoxTransform(String point2DSQL) {
		return internalDefinition.doPoint2DGetBoundingBoxTransform(point2DSQL);
	}

	@Override
	public String doPoint2DAsTextTransform(String point2DSQL) {
		return internalDefinition.doPoint2DAsTextTransform(point2DSQL);
	}

	@Override
	public String transformPoint2DIntoDatabaseFormat(Point point) {
		return internalDefinition.transformPoint2DIntoDatabaseFormat(point);
	}

	@Override
	public String transformCoordinatesIntoDatabasePoint2DFormat(String xValue, String yValue) {
		return internalDefinition.transformCoordinatesIntoDatabasePoint2DFormat(xValue, yValue);
	}

	@Override
	public Point transformDatabasePoint2DValueToJTSPoint(String pointAsString) throws com.vividsolutions.jts.io.ParseException {
		return internalDefinition.transformDatabasePoint2DValueToJTSPoint(pointAsString);
	}

	@Override
	public Polygon transformDatabasePolygon2DToJTSPolygon(String polygon2DSQL) throws com.vividsolutions.jts.io.ParseException {
		return internalDefinition.transformDatabasePolygon2DToJTSPolygon(polygon2DSQL);
	}

	@Override
	public LineString transformDatabaseLine2DValueToJTSLineString(String lineStringAsSQL) throws com.vividsolutions.jts.io.ParseException {
		return internalDefinition.transformDatabaseLine2DValueToJTSLineString(lineStringAsSQL);
	}

	@Override
	public String transformLineStringIntoDatabaseLine2DFormat(LineString lineString) {
		return internalDefinition.transformLineStringIntoDatabaseLine2DFormat(lineString);
	}

	@Override
	public String doLine2DAsTextTransform(String line2DSQL) {
		return internalDefinition.doLine2DAsTextTransform(line2DSQL);
	}

	@Override
	public String doLine2DEqualsTransform(String line2DSQL, String otherLine2DSQL) {
		return internalDefinition.doLine2DEqualsTransform(line2DSQL, otherLine2DSQL);
	}

	@Override
	public String doLine2DNotEqualsTransform(String line2DSQL, String otherLine2DSQL) {
		return internalDefinition.doLine2DNotEqualsTransform(line2DSQL, otherLine2DSQL);
	}

	@Override
	public String doLine2DMeasurableDimensionsTransform(String line2DSQL) {
		return internalDefinition.doLine2DMeasurableDimensionsTransform(line2DSQL);
	}

	@Override
	public String doLine2DGetBoundingBoxTransform(String line2DSQL) {
		return internalDefinition.doLine2DGetBoundingBoxTransform(line2DSQL);
	}

	@Override
	public String transformPoint2DArrayToDatabasePolygon2DFormat(List<String> pointSQL) {
		return internalDefinition.transformPoint2DArrayToDatabasePolygon2DFormat(pointSQL);
	}

	@Override
	public String doLine2DGetMaxXTransform(String line2DSQL) {
		return internalDefinition.doLine2DGetMaxXTransform(line2DSQL);
	}

	@Override
	public String doLine2DGetMinXTransform(String line2DSQL) {
		return internalDefinition.doLine2DGetMinXTransform(line2DSQL);
	}

	@Override
	public String doLine2DGetMaxYTransform(String line2DSQL) {
		return internalDefinition.doLine2DGetMaxYTransform(line2DSQL);
	}

	@Override
	public String doLine2DGetMinYTransform(String line2DSQL) {
		return internalDefinition.doLine2DGetMinYTransform(line2DSQL);
	}

	@Override
	public String doPolygon2DGetMaxXTransform(String polygon2DSQL) {
		return internalDefinition.doPolygon2DGetMaxXTransform(polygon2DSQL);
	}

	@Override
	public String doPolygon2DGetMinXTransform(String polygon2DSQL) {
		return internalDefinition.doPolygon2DGetMinXTransform(polygon2DSQL);
	}

	@Override
	public String doPolygon2DGetMaxYTransform(String polygon2DSQL) {
		return internalDefinition.doPolygon2DGetMaxYTransform(polygon2DSQL);
	}

	@Override
	public String doPolygon2DGetMinYTransform(String polygon2DSQL) {
		return internalDefinition.doPolygon2DGetMinYTransform(polygon2DSQL);
	}

	@Override
	public String transformPolygonIntoDatabasePolygon2DFormat(Polygon polygon2D) {
		return internalDefinition.transformPolygonIntoDatabasePolygon2DFormat(polygon2D);
	}

	@Override
	public String doPoint2DDistanceBetweenTransform(String polygon2DSQL, String otherPolygon2DSQL) {
		return internalDefinition.doPoint2DDistanceBetweenTransform(polygon2DSQL, otherPolygon2DSQL);
	}

	@Override
	public String doRoundTransform(String numberSQL) {
		return internalDefinition.doRoundTransform(numberSQL);
	}

	@Override
	public String doRoundWithDecimalPlacesTransform(String number, String decimalPlaces) {
		return internalDefinition.doRoundWithDecimalPlacesTransform(number, decimalPlaces);
	}

	@Override
	public String doSubstringBeforeTransform(String fromThis, String beforeThis) {
		return internalDefinition.doSubstringBeforeTransform(fromThis, beforeThis);
	}

	@Override
	public String doSubstringAfterTransform(String fromThis, String afterThis) {
		return internalDefinition.doSubstringAfterTransform(fromThis, afterThis);
	}

	@Override
	public boolean willCloseConnectionOnStatementCancel() {
		return internalDefinition.willCloseConnectionOnStatementCancel();
	}

	@Override
	public boolean supportsStatementIsClosed() {
		return internalDefinition.supportsStatementIsClosed();
	}

	@Override
	public String doPolygon2DContainsPoint2DTransform(String polygon2DSQL, String point2DSQL) {
		return internalDefinition.doPolygon2DContainsPoint2DTransform(polygon2DSQL, point2DSQL);
	}

	@Override
	public String doPolygon2DAsTextTransform(String polygonSQL) {
		return internalDefinition.doPolygon2DAsTextTransform(polygonSQL);
	}

	@Override
	public String doLine2DIntersectsLine2DTransform(String firstLine, String secondLine) {
		return internalDefinition.doLine2DIntersectsLine2DTransform(firstLine, secondLine);
	}

	@Override
	public String doLine2DIntersectionPointWithLine2DTransform(String firstLine, String secondLine) {
		return internalDefinition.doLine2DIntersectionPointWithLine2DTransform(firstLine, secondLine);
	}

	@Override
	public String doLine2DAllIntersectionPointsWithLine2DTransform(String firstLine, String secondLine) {
		return internalDefinition.doLine2DAllIntersectionPointsWithLine2DTransform(firstLine, secondLine);
	}

	@Override
	public LineSegment transformDatabaseLineSegment2DValueToJTSLineSegment(String lineSegmentAsSQL) throws com.vividsolutions.jts.io.ParseException {
		return internalDefinition.transformDatabaseLineSegment2DValueToJTSLineSegment(lineSegmentAsSQL);
	}

	@Override
	public String transformLineSegmentIntoDatabaseLineSegment2DFormat(LineSegment lineSegment) {
		return internalDefinition.transformLineSegmentIntoDatabaseLineSegment2DFormat(lineSegment);
	}

	@Override
	public String doLineSegment2DIntersectsLineSegment2DTransform(String firstSQL, String secondSQL) {
		return internalDefinition.doLineSegment2DIntersectsLineSegment2DTransform(firstSQL, secondSQL);
	}

	@Override
	public String doLineSegment2DGetMaxXTransform(String lineSegment) {
		return internalDefinition.doLineSegment2DGetMaxXTransform(lineSegment);
	}

	@Override
	public String doLineSegment2DGetMinXTransform(String lineSegment) {
		return internalDefinition.doLineSegment2DGetMinXTransform(lineSegment);
	}

	@Override
	public String doLineSegment2DGetMaxYTransform(String lineSegment) {
		return internalDefinition.doLineSegment2DGetMaxYTransform(lineSegment);
	}

	@Override
	public String doLineSegment2DGetMinYTransform(String lineSegment) {
		return internalDefinition.doLineSegment2DGetMinYTransform(lineSegment);
	}

	@Override
	public String doLineSegment2DGetBoundingBoxTransform(String lineSegment) {
		return internalDefinition.doLineSegment2DGetBoundingBoxTransform(lineSegment);
	}

	@Override
	public String doLineSegment2DDimensionTransform(String lineSegment) {
		return internalDefinition.doLineSegment2DDimensionTransform(lineSegment);
	}

	@Override
	public String doLineSegment2DNotEqualsTransform(String firstLineSegment, String secondLineSegment) {
		return internalDefinition.doLineSegment2DNotEqualsTransform(firstLineSegment, secondLineSegment);
	}

	@Override
	public String doLineSegment2DEqualsTransform(String firstLineSegment, String secondLineSegment) {
		return internalDefinition.doLineSegment2DEqualsTransform(firstLineSegment, secondLineSegment);
	}

	@Override
	public String doLineSegment2DAsTextTransform(String lineSegment) {
		return internalDefinition.doLineSegment2DAsTextTransform(lineSegment);
	}

	@Override
	public String doLineSegment2DIntersectionPointWithLineSegment2DTransform(String firstLineSegment, String secondLineSegment) {
		return internalDefinition.doLineSegment2DIntersectionPointWithLineSegment2DTransform(firstLineSegment, secondLineSegment);
	}

	@Override
	public String doLineSegment2DStartPointTransform(String lineSegmentSQL) {
		return internalDefinition.doLineSegment2DStartPointTransform(lineSegmentSQL);
	}

	@Override
	public String doLineSegment2DEndPointTransform(String lineSegmentSQL) {
		return internalDefinition.doLineSegment2DEndPointTransform(lineSegmentSQL);
	}

	@Override
	public String transformMultiPoint2DToDatabaseMultiPoint2DValue(MultiPoint points) {
		return internalDefinition.transformMultiPoint2DToDatabaseMultiPoint2DValue(points);
	}

	@Override
	public MultiPoint transformDatabaseMultiPoint2DValueToJTSMultiPoint(String pointsAsString) throws com.vividsolutions.jts.io.ParseException {
		return internalDefinition.transformDatabaseMultiPoint2DValueToJTSMultiPoint(pointsAsString);
	}

	@Override
	public String doMultiPoint2DEqualsTransform(String firstMultiPointValue, String secondMultiPointValue) {
		return internalDefinition.doMultiPoint2DEqualsTransform(firstMultiPointValue, secondMultiPointValue);
	}

	@Override
	public String doMultiPoint2DNotEqualsTransform(String first, String second) {
		return internalDefinition.doMultiPoint2DNotEqualsTransform(first, second);
	}

	@Override
	public String doMultiPoint2DGetPointAtIndexTransform(String first, String index) {
		return internalDefinition.doMultiPoint2DGetPointAtIndexTransform(first, index);
	}

	@Override
	public String doMultiPoint2DGetNumberOfPointsTransform(String multiPoint2D) {
		return internalDefinition.doMultiPoint2DGetNumberOfPointsTransform(multiPoint2D);
	}

	@Override
	public String doMultiPoint2DMeasurableDimensionsTransform(String multipoint2D) {
		return internalDefinition.doMultiPoint2DMeasurableDimensionsTransform(multipoint2D);
	}

	@Override
	public String doMultiPoint2DGetBoundingBoxTransform(String multiPoint2D) {
		return internalDefinition.doMultiPoint2DGetBoundingBoxTransform(multiPoint2D);
	}

	@Override
	public String doMultiPoint2DAsTextTransform(String multiPoint2D) {
		return internalDefinition.doMultiPoint2DAsTextTransform(multiPoint2D);
	}

	@Override
	public String doMultiPoint2DToLine2DTransform(String multiPoint2D) {
		return internalDefinition.doMultiPoint2DToLine2DTransform(multiPoint2D);
	}

	@Override
	public String doMultiPoint2DGetMinYTransform(String multiPoint2D) {
		return internalDefinition.doMultiPoint2DGetMinYTransform(multiPoint2D);
	}

	@Override
	public String doMultiPoint2DGetMinXTransform(String multiPoint2D) {
		return internalDefinition.doMultiPoint2DGetMinXTransform(multiPoint2D);
	}

	@Override
	public String doMultiPoint2DGetMaxYTransform(String multiPoint2D) {
		return internalDefinition.doMultiPoint2DGetMaxYTransform(multiPoint2D);
	}

	@Override
	public String doMultiPoint2DGetMaxXTransform(String multiPoint2D) {
		return internalDefinition.doMultiPoint2DGetMaxXTransform(multiPoint2D);
	}

	@Override
	public boolean supportsRowLimitsNatively(QueryOptions options) {
		return internalDefinition.supportsRowLimitsNatively(options);
	}

	@Override
	public boolean requiresSpatial2DIndexes() {
		return internalDefinition.requiresSpatial2DIndexes();
	}

	@Override
	public List<String> getSpatial2DIndexSQL(DBDatabase database, String formatTableName, String formatColumnName) {
		return internalDefinition.getSpatial2DIndexSQL(database, formatTableName, formatColumnName);
	}

	@Override
	public String doWrapQueryForPaging(String sqlQuery, QueryOptions options) {
		return internalDefinition.doWrapQueryForPaging(sqlQuery, options);
	}

	@Override
	public String doLine2DSpatialDimensionsTransform(String line2DSQL) {
		return internalDefinition.doLine2DSpatialDimensionsTransform(line2DSQL);
	}

	@Override
	public String doLine2DHasMagnitudeTransform(String line2DSQL) {
		return internalDefinition.doLine2DHasMagnitudeTransform(line2DSQL);
	}

	@Override
	public String doLine2DGetMagnitudeTransform(String line2DSQL) {
		return internalDefinition.doLine2DGetMagnitudeTransform(line2DSQL);
	}

	@Override
	public String doPoint2DSpatialDimensionsTransform(String point2DSQL) {
		return internalDefinition.doPoint2DSpatialDimensionsTransform(point2DSQL);
	}

	@Override
	public String doPoint2DHasMagnitudeTransform(String point2DSQL) {
		return internalDefinition.doPoint2DHasMagnitudeTransform(point2DSQL);
	}

	@Override
	public String doPoint2DGetMagnitudeTransform(String point2DSQL) {
		return internalDefinition.doPoint2DGetMagnitudeTransform(point2DSQL);
	}

	@Override
	public String doMultiPoint2DSpatialDimensionsTransform(String multipoint2DSQL) {
		return internalDefinition.doMultiPoint2DSpatialDimensionsTransform(multipoint2DSQL);
	}

	@Override
	public String doMultiPoint2DHasMagnitudeTransform(String multipoint2DSQL) {
		return internalDefinition.doMultiPoint2DHasMagnitudeTransform(multipoint2DSQL);
	}

	@Override
	public String doMultiPoint2DGetMagnitudeTransform(String multipoint2DSQL) {
		return internalDefinition.doMultiPoint2DGetMagnitudeTransform(multipoint2DSQL);
	}

	@Override
	public String doPolygon2DSpatialDimensionsTransform(String polygon2DSQL) {
		return internalDefinition.doPolygon2DSpatialDimensionsTransform(polygon2DSQL);
	}

	@Override
	public String doPolygon2DHasMagnitudeTransform(String polygon2DSQL) {
		return internalDefinition.doPolygon2DHasMagnitudeTransform(polygon2DSQL);
	}

	@Override
	public String doPolygon2DGetMagnitudeTransform(String polygon2DSQL) {
		return internalDefinition.doPolygon2DGetMagnitudeTransform(polygon2DSQL);
	}

	@Override
	public String doLineSegment2DSpatialDimensionsTransform(String lineSegment2DSQL) {
		return internalDefinition.doLineSegment2DSpatialDimensionsTransform(lineSegment2DSQL);
	}

	@Override
	public String doLineSegment2DHasMagnitudeTransform(String lineSegment2DSQL) {
		return internalDefinition.doLineSegment2DHasMagnitudeTransform(lineSegment2DSQL);
	}

	@Override
	public String doLineSegment2DGetMagnitudeTransform(String lineSegment2DSQL) {
		return internalDefinition.doLineSegment2DGetMagnitudeTransform(lineSegment2DSQL);
	}

	@Override
	public String transformCoordinateArrayToDatabasePolygon2DFormat(List<String> coordinateSQL) {
		return internalDefinition.transformCoordinateArrayToDatabasePolygon2DFormat(coordinateSQL);
	}

	@Override
	public String doEndOfMonthTransform(String dateSQL) {
		return internalDefinition.doEndOfMonthTransform(dateSQL);
	}

	@Override
	public String doInstantEndOfMonthTransform(String dateSQL) {
		return internalDefinition.doInstantEndOfMonthTransform(dateSQL);
	}

	@Override
	public String doDateAtTimeZoneTransform(String dateSQL, TimeZone timeZone) throws UnsupportedOperationException {
		return internalDefinition.doDateAtTimeZoneTransform(dateSQL, timeZone);
	}

	@Override
	public Class<? extends QueryableDatatype<?>> getQueryableDatatypeClassForSQLDatatype(String typeName) {
		return internalDefinition.getQueryableDatatypeClassForSQLDatatype(typeName);
	}

	@Override
	public String getHavingClauseStart() {
		return internalDefinition.getHavingClauseStart();
	}

	@Override
	public String getTrueValue() {
		return internalDefinition.getTrueValue();
	}

	@Override
	public String getFalseValue() {
		return internalDefinition.getFalseValue();
	}

	@Override
	public String doBooleanStatementToBooleanComparisonValueTransform(String booleanStatement) {
		return internalDefinition.doBooleanStatementToBooleanComparisonValueTransform(booleanStatement);
	}

	@Override
	public String doBooleanValueToBooleanComparisonValueTransform(String booleanValueSQL) {
		return internalDefinition.doBooleanValueToBooleanComparisonValueTransform(booleanValueSQL);
	}

	@Override
	public String getUnionDistinctOperator() {
		return internalDefinition.getUnionDistinctOperator();
	}

	@Override
	public String getUnionOperator() {
		return internalDefinition.getUnionOperator();
	}

	@Override
	public LargeObjectHandlerType preferredLargeObjectWriter(DBLargeObject<?> lob) {
		return internalDefinition.preferredLargeObjectWriter(lob);
	}

	@Override
	public LargeObjectHandlerType preferredLargeObjectReader(DBLargeObject<?> lob) {
		return internalDefinition.preferredLargeObjectReader(lob);
	}

	@Override
	public String getRoundUpFunctionName() {
		return internalDefinition.getRoundUpFunctionName();
	}

	@Override
	public String getNaturalLogFunctionName() {
		return internalDefinition.getNaturalLogFunctionName();
	}

	@Override
	public String getLogBase10FunctionName() {
		return internalDefinition.getLogBase10FunctionName();
	}

	@Override
	public String doRandomNumberTransform() {
		return internalDefinition.doRandomNumberTransform();
	}

	@Override
	public String doRandomIntegerTransform() {
		return internalDefinition.doRandomIntegerTransform();
	}

	@Override
	public String doLogBase10NumberTransform(String sql) {
		return internalDefinition.doLogBase10NumberTransform(sql);
	}

	@Override
	public String doLogBase10IntegerTransform(String sql) {
		return internalDefinition.doLogBase10IntegerTransform(sql);
	}

	@Override
	public String doNumberToIntegerTransform(String sql) {
		return internalDefinition.doNumberToIntegerTransform(sql);
	}

	@Override
	public String doFindNumberInStringTransform(String toSQLString) {
		return internalDefinition.doFindNumberInStringTransform(toSQLString);
	}

	@Override
	public String doFindIntegerInStringTransform(String toSQLString) {
		return internalDefinition.doFindIntegerInStringTransform(toSQLString);
	}

	@Override
	public String doIntegerToNumberTransform(String toSQLString) {
		return internalDefinition.doIntegerToNumberTransform(toSQLString);
	}

	@Override
	public boolean persistentConnectionRequired() {
		return internalDefinition.persistentConnectionRequired();
	}

	@Override
	public Boolean supportsDifferenceBetweenNullAndEmptyStringNatively() {
		return internalDefinition.supportsDifferenceBetweenNullAndEmptyStringNatively();
	}

	@Override
	public Boolean supportsUnionDistinct() {
		return internalDefinition.supportsUnionDistinct();
	}

	@Override
	public boolean supportsRecursiveQueriesNatively() {
		return internalDefinition.supportsRecursiveQueriesNatively();
	}

	@Override
	public boolean supportsFullOuterJoin() {
		return internalDefinition.supportsFullOuterJoin();
	}

	@Override
	public boolean supportsFullOuterJoinNatively() {
		return internalDefinition.supportsFullOuterJoinNatively();
	}

	@Override
	public boolean supportsRightOuterJoinNatively() {
		return internalDefinition.supportsRightOuterJoinNatively();
	}

	@Override
	public boolean supportsAlterTableAddConstraint() {
		return internalDefinition.supportsAlterTableAddConstraint();
	}

	@Override
	public String getSQLToCheckTableExists(DBRow table) {
		return internalDefinition.getSQLToCheckTableExists(table);
	}

	@Override
	public boolean supportsTableCheckingViaMetaData() {
		return internalDefinition.supportsTableCheckingViaMetaData();
	}

	@Override
	public boolean requiresOnClauseForAllJoins() {
		return internalDefinition.requiresOnClauseForAllJoins();
	}

	@Override
	public boolean requiresSequenceUpdateAfterManualInsert() {
		return internalDefinition.requiresSequenceUpdateAfterManualInsert();
	}

	@Override
	public String getSequenceUpdateSQL(String tableName, String columnName, long primaryKeyGenerated) {
		return internalDefinition.getSequenceUpdateSQL(tableName, columnName, primaryKeyGenerated);
	}

	@Override
	public Collection<? extends String> getInsertPreparation(DBRow table) {
		return internalDefinition.getInsertPreparation(table);
	}

	@Override
	public Collection<? extends String> getInsertCleanUp(DBRow table) {
		return internalDefinition.getInsertCleanUp(table);
	}

	@Override
	public String getAlterTableAddColumnSQL(DBRow existingTable, PropertyWrapper columnPropertyWrapper) {
		return internalDefinition.getAlterTableAddColumnSQL(existingTable, columnPropertyWrapper);
	}

	@Override
	public String getAddColumnColumnSQL(PropertyWrapper field) {
		return internalDefinition.getAddColumnColumnSQL(field);
	}

	@Override
	public boolean supportsNullsOrderingStandard() {
		return internalDefinition.supportsNullsOrderingStandard();
	}

	@Override
	public String getNullsLast() {
		return internalDefinition.getNullsLast();
	}

	@Override
	public String getNullsFirst() {
		return internalDefinition.getNullsFirst();
	}

	@Override
	public String getNullsAnyOrder() {
		return internalDefinition.getNullsAnyOrder();
	}

	@Override
	public String getTableExistsSQL(DBRow table) {
		return internalDefinition.getTableExistsSQL(table);
	}

	@Override
	public boolean supportsDropTableIfExists() {
		return internalDefinition.supportsDropTableIfExists();
	}

	@Override
	public String getDropTableIfExistsClause() {
		return internalDefinition.getDropTableIfExistsClause();
	}

	@Override
	public String doStringAccumulateTransform(String accumulateColumn, String separator, String referencedTable) {
		return internalDefinition.doStringAccumulateTransform(accumulateColumn, separator, referencedTable);
	}

	@Override
	public String doStringAccumulateTransform(String accumulateColumn, String separator, String orderByColumnName, String referencedTable) {
		return internalDefinition.doStringAccumulateTransform(accumulateColumn, separator, orderByColumnName, referencedTable);
	}

	@Override
	public boolean requiresSortedSubselectForStringAggregate() {
		return internalDefinition.requiresSortedSubselectForStringAggregate();
	}

	@Override
	public String doStringAccumulateTransform(StringExpression columnToAccumulate, StringExpression separator, SortProvider orderBy) {
		return internalDefinition.doStringAccumulateTransform(columnToAccumulate, separator, orderBy);
	}

	@Override
	public boolean requiresClosedPolygons() {
		return internalDefinition.requiresClosedPolygons();
	}

	@Override
	public boolean requiresReversingLineStringsFromDatabase() {
		return internalDefinition.requiresReversingLineStringsFromDatabase();
	}

	@Override
	public DBExpression transformToSelectableType(DBExpression expression) {
		return internalDefinition.transformToSelectableType(expression);
	}

	@Override
	public DBExpression transformToGroupableType(DBExpression expression) {
		return internalDefinition.transformToGroupableType(expression);
	}

	@Override
	public boolean supportsBulkInserts() {
		return internalDefinition.supportsBulkInserts();
	}

	@Override
	public boolean supportsWindowingFunctionsInTheHavingClause() {
		return internalDefinition.supportsWindowingFunctionsInTheHavingClause();
	}

	@Override
	public boolean supportsWindowingFunctionsInTheOrderByClause() {
		return internalDefinition.supportsWindowingFunctionsInTheOrderByClause();
	}

	@Override
	public String getRowNumberFunctionName() {
		return internalDefinition.getRowNumberFunctionName();
	}

	@Override
	public String getDenseRankFunctionName() {
		return internalDefinition.getDenseRankFunctionName();
	}

	@Override
	public String getRankFunctionName() {
		return internalDefinition.getRankFunctionName();
	}

	@Override
	public String getNTilesFunctionName() {
		return internalDefinition.getNTilesFunctionName();
	}

	@Override
	public String getPercentRankFunctionName() {
		return internalDefinition.getPercentRankFunctionName();
	}

	@Override
	public String getFirstValueFunctionName() {
		return internalDefinition.getFirstValueFunctionName();
	}

	@Override
	public String getLastValueFunctionName() {
		return internalDefinition.getLastValueFunctionName();
	}

	@Override
	public String getNthValueFunctionName() {
		return internalDefinition.getNthValueFunctionName();
	}

	@Override
	public String doNewLocalDateFromYearMonthDayTransform(String years, String months, String days) {
		return internalDefinition.doNewLocalDateFromYearMonthDayTransform(years, months, days);
	}

	@Override
	public String doLeftPadTransform(String toPad, String padWith, String length) {
		return internalDefinition.doLeftPadTransform(toPad, padWith, length);
	}

	@Override
	public boolean supportsLeftPadTransform() {
		return internalDefinition.supportsLeftPadTransform();
	}

	@Override
	public String doRightPadTransform(String toPad, String padWith, String length) {
		return internalDefinition.doRightPadTransform(toPad, padWith, length);
	}

	@Override
	public boolean supportsRightPadTransform() {
		return internalDefinition.supportsRightPadTransform();
	}

	@Override
	public String doCurrentUTCDateTimeTransform() {
		return internalDefinition.doCurrentUTCDateTimeTransform();
	}

	@Override
	public boolean supportsTimeZones() {
		return internalDefinition.supportsTimeZones();
	}

	@Override
	public String getLagFunctionName() {
		return internalDefinition.getLagFunctionName();
	}

	@Override
	public String getLeadFunctionName() {
		return internalDefinition.getLeadFunctionName();
	}

	@Override
	public DBExpression transformToWhenableType(BooleanExpression test) {
		return internalDefinition.transformToWhenableType(test);
	}

	@Override
	public String getDefaultOrderingClause() {
		return internalDefinition.getDefaultOrderingClause();
	}

	@Override
	public String transformJavaDurationIntoDatabaseDuration(Duration interval) {
		return internalDefinition.transformJavaDurationIntoDatabaseDuration(interval);
	}

	@Override
	public Duration parseDurationFromGetString(String intervalStr) {
		return internalDefinition.parseDurationFromGetString(intervalStr);
	}

	@Override
	public String doDurationLessThanTransform(String toSQLString, String toSQLString0) {
		return internalDefinition.doDurationLessThanTransform(toSQLString, toSQLString0);
	}

	@Override
	public String doDurationGreaterThanTransform(String toSQLString, String toSQLString0) {
		return internalDefinition.doDurationGreaterThanTransform(toSQLString, toSQLString0);
	}

	@Override
	public String doDurationLessThanEqualsTransform(String toSQLString, String toSQLString0) {
		return internalDefinition.doDurationLessThanEqualsTransform(toSQLString, toSQLString0);
	}

	@Override
	public String doDurationGreaterThanEqualsTransform(String toSQLString, String toSQLString0) {
		return internalDefinition.doDurationGreaterThanEqualsTransform(toSQLString, toSQLString0);
	}

	@Override
	public String doDurationEqualsTransform(String toSQLString, String toSQLString0) {
		return internalDefinition.doDurationEqualsTransform(toSQLString, toSQLString0);
	}

	@Override
	public String doDatePlusDurationTransform(String toSQLString, String toSQLString0) {
		return internalDefinition.doDatePlusDurationTransform(toSQLString, toSQLString0);
	}

	@Override
	public String doDateMinusDurationTransform(String toSQLString, String toSQLString0) {
		return internalDefinition.doDateMinusDurationTransform(toSQLString, toSQLString0);
	}

	@Override
	public boolean supportsDurationNatively() {
		return internalDefinition.supportsDurationNatively();
	}

	@Override
	public int getParseDurationPartOffset() {
		return internalDefinition.getParseDurationPartOffset();
	}

	@Override
	public String convertNullToEmptyString(String toSQLString) {
		return internalDefinition.convertNullToEmptyString(toSQLString);
	}

	@Override
	public String doIsNullTransform(String expressionSQL) {
		return internalDefinition.doIsNullTransform(expressionSQL);
	}
	
}
