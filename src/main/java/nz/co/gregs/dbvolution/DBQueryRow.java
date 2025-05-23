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
package nz.co.gregs.dbvolution;

import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.*;
import nz.co.gregs.dbvolution.internal.query.QueryDetails;
import nz.co.gregs.separatedstring.Builder;
import nz.co.gregs.separatedstring.Encoder;

/**
 * Contains all the instances of DBRow that are associated with one line of a
 * DBQuery request.
 *
 * <p>
 * DBQueryRow represents an individual line within the results of a query.
 * However the results within the line are contained in instances of all the
 * DBRow subclasses included in the DBQuery.
 *
 * <p>
 * Each instance is accessible thru the
 * {@link #get(nz.co.gregs.dbvolution.DBRow) get(DBRow) method}.
 *
 * @author Gregory Graham
 *
 */
public class DBQueryRow extends HashMap<Class<? extends DBRow>, DBRow> {

	private static final long serialVersionUID = 1;
	private final LinkedHashMap<Object, QueryableDatatype<?>> expressionColumnValues = new LinkedHashMap<>();
	private transient final QueryDetails baseQuery;

	public DBQueryRow(QueryDetails queryThatThisRowWasGeneratedFor) {
		super();
		baseQuery = queryThatThisRowWasGeneratedFor;
	}

	/**
	 * Returns the instance of exemplar contained within this DBQueryRow.
	 *
	 * <p>
	 * Finds the instance of the class supplied that is relevant to the DBRow and
	 * returns it.
	 *
	 * <p>
	 * If the exemplar represents an optional table an there were no appropriate
	 * rows found for that table then NULL will be returned.
	 *
	 * <p>
	 * Criteria set on the exemplar are ignored.
	 *
	 * <p>
	 * For example: Marque thisMarque = myQueryRow.get(new Marque());
	 *
	 * @param <E> DBRow type
	 * @param exemplar exemplar
	 *
	 * @return the instance of exemplar that is in the DBQueryRow instance
	 */
	@SuppressWarnings("unchecked")
	public <E extends DBRow> E get(E exemplar) {
		return (E) get(exemplar.getClass());
	}

	/**
	 * Returns the all DBRow instances contained within this DBQueryRow.
	 *
	 * @return all DBRow instances that are in this DBQueryRow instance
	 */
	public List<DBRow> getAll() {
		final ArrayList<DBRow> arrayList = new ArrayList<>();
		arrayList.addAll(this.values());
		return arrayList;
	}

	/**
	 * Print the specified columns to the specified PrintStream as one line.
	 *
	 * @param ps ps
	 * @param columns columns
	 */
	public void print(PrintStream ps, QueryableDatatype<?>... columns) {
		for (QueryableDatatype<?> qdt : columns) {
			ps.print("" + qdt + " ");
		}
		ps.println();
	}

	/**
	 * Print the all columns to the specified PrintStream as one line.
	 *
	 * @param ps	ps
	 */
	public void print(PrintStream ps) {
		values().forEach(row -> ps.print("" + row));
	}

	public void addExpressionColumnValue(Object key, QueryableDatatype<?> expressionQDT) {
		expressionColumnValues.put(key, expressionQDT);
	}

	public QueryableDatatype<?> getExpressionColumnValue(Object key) {
		return expressionColumnValues.get(key);
	}

	public Map<Object, QueryableDatatype<?>> getExpressionColumns() {
		return expressionColumnValues;
	}

	/**
	 * Returns all the fields names from all the DBRow types in the DBQueryRow.
	 *
	 * <p>
	 * This is essentially a list of all the columns returned from the database
	 * query.
	 *
	 * <p>
	 * Column data may not have been populated.
	 *
	 * <p>
	 * Please note this is a crude instrument for accessing the data in this
	 * DBQueryRow. You should probably be using {@link DBQueryRow#get(nz.co.gregs.dbvolution.DBRow)
	 * } and using the fields and methods of the individual DBRow classes.
	 *
	 * @return a list of field names.
	 */
	public List<String> getFieldNames() {
		List<String> returnList = new ArrayList<String>();
		for (DBRow tab : baseQuery.getAllQueryTables()) {
			Collection<? extends String> fieldNames = tab.getFieldNames();
			for (String fieldName : fieldNames) {
				returnList.add(tab.getClass().getSimpleName() + ":" + fieldName);
			}
		}
		return returnList;
	}

	/**
	 * Returns all the fields values from all the DBRow types in the DBQueryRow.
	 *
	 * <p>
	 * This is essentially a list of all the values returned from the database
	 * query.
	 *
	 * <p>
	 * Please note this is a crude instrument for accessing the data in this
	 * DBQueryRow. You should probably be using {@link DBQueryRow#get(nz.co.gregs.dbvolution.DBRow)
	 * } and using the fields and methods of the individual DBRow classes.
	 *
	 * @param dateFormat format that date should be formatted to.
	 * @return a list of field names.
	 *
	 */
	public List<String> getFieldValues(SimpleDateFormat dateFormat) {
		List<String> returnList = new ArrayList<String>();

		for (DBRow tab : baseQuery.getAllQueryTables()) {
			DBRow actualRow = this.get(tab);
			if (actualRow != null) {
				returnList.addAll(actualRow.getFieldValues(dateFormat));
			} else {
				for (String returnList1 : tab.getFieldNames()) {
					returnList.add(returnList1);
				}
			}
		}
		return returnList;
	}

	/**
	 * Convenience method to convert this DBQueryRow into a CSV or TSV type
	 * header.
	 *
	 * <p>
	 * The line separator is not included in the results, to allow for portability
	 * and post-processing.
	 *
	 * @param separatorToUseBetweenValues	separatorToUseBetweenValues
	 * @return a list of all the fields in the DBQueryRow separated by the
	 * supplied value
	 */
	public String toSeparatedHeader(String separatorToUseBetweenValues) {
		StringBuilder returnStr = new StringBuilder();
		String separator = "";
		List<String> fieldNames = this.getFieldNames();
		for (String fieldName : fieldNames) {
			returnStr.append(separator).append("\"").append(fieldName.replaceAll("\"", "\"\"")).append("\"");
			separator = separatorToUseBetweenValues;
		}
		return returnStr.toString();
	}

	/**
	 * Convenience method to convert this DBQueryRow into a CSV or TSV type line.
	 *
	 * <p>
	 * The line separator is not included in the results, to allow for portability
	 * and post-processing.
	 *
	 * @param separatorToUseBetweenValues	separatorToUseBetweenValues
	 * @return a list of all the values in the DBQueryRow formatted for a TSV or
	 * CSV file
	 */
	public String toSeparatedLine(String separatorToUseBetweenValues) {
		return toSeparatedLine(separatorToUseBetweenValues, null);
	}

	/**
	 * Convenience method to convert this DBQueryRow into a CSV or TSV type line.
	 *
	 * <p>
	 * The line separator is not included in the results, to allow for portability
	 * and post-processing.
	 *
	 * @param separatorToUseBetweenValues	separatorToUseBetweenValues
	 * @param dateFormat format that dates should be formatted to.
	 * @return a list of all the values in the DBQueryRow formatted for a TSV or
	 * CSV file
	 */
	public String toSeparatedLine(String separatorToUseBetweenValues, SimpleDateFormat dateFormat) {
		StringBuilder returnStr = new StringBuilder();
		String separator = "";
		List<String> fieldValues = this.getFieldValues(dateFormat);
		for (String fieldValue : fieldValues) {
			returnStr.append(separator).append("\"").append(fieldValue.replaceAll("\"", "\"\"")).append("\"");
			separator = separatorToUseBetweenValues;
		}
		return returnStr.toString();
	}

	/**
	 * Convenience method to convert this DBQueryRow into a CSV file's header.
	 *
	 * <p>
	 * The line separator is not included in the results, to allow for portability
	 * and post-processing.
	 *
	 * @return a list of all the fields in the DBQueryRow formatted for a CSV file
	 */
	public String toCSVHeader() {
		return toSeparatedHeader(",");
	}

	/**
	 * Convenience method to convert this DBQueryRow into a CSV line.
	 *
	 * <p>
	 * The line separator is not included in the results, to allow for portability
	 * and post-processing.
	 *
	 * @return a list of all the values in the DBQueryRow formatted for a CSV file
	 */
	public String toCSVLine() {
		return toSeparatedLine(",");
	}

	/**
	 * Convenience method to convert this DBQueryRow into a CSV line.
	 *
	 * <p>
	 * The line separator is not included in the results, to allow for portability
	 * and post-processing.
	 *
	 * @param dateFormat	dateFormat
	 * @return a list of all the values in the DBQueryRow formatted for a CSV file
	 */
	public String toCSVLine(SimpleDateFormat dateFormat) {
		return toSeparatedLine(",", dateFormat);
	}

	/**
	 * Convenience method to convert this DBQueryRow into a Tab Separated Values
	 * file's header.
	 *
	 * <p>
	 * The line separator is not included in the results, to allow for portability
	 * and post-processing.
	 *
	 * @return a list of all the fields in the DBQueryRow formatted for a TSV file
	 */
	public String toTabbedHeader() {
		return toSeparatedHeader("\t");
	}

	/**
	 * Convenience method to convert this DBQueryRow into a Tab Separated Values
	 * line.
	 * <p>
	 * The line separator is not included in the results, to allow for portability
	 * and post-processing.
	 *
	 * @return a list of all the values in the DBQueryRow formatted for a TSV file
	 * @throws java.lang.IllegalAccessException java.lang.IllegalAccessException
	 *
	 */
	public String toTabbedLine() throws IllegalArgumentException, IllegalAccessException {
		return toSeparatedLine("\t");
	}

	@Override
	public DBQueryRow clone() {
		return (DBQueryRow) super.clone();
	}

	@Override
	public String toString() {
		Encoder encoder = Builder
            .forSeparator(", ")
            .withKeyValueSeparator("=")
            .withPrefix("{")
            .withSuffix("}")
            .useWhenEmpty("{}")
            .encoder();

		var entrySet = entrySet();
		ArrayList<Entry<Class<? extends DBRow>, DBRow>> entryList = new ArrayList<>(entrySet);
		entryList.sort((original, other) -> {
			return original.getKey().getCanonicalName().compareTo(other.getKey().getCanonicalName());
		});
		for (Entry<Class<? extends DBRow>, DBRow> entry : entryList) {
			encoder.add(entry.getKey().toString(),entry.getValue().toString());
		}

		var entrySet2 = expressionColumnValues.entrySet();
		ArrayList<Entry<Object, QueryableDatatype<?>>> entryList2 = new ArrayList<>(entrySet2);
		entryList2.sort((original, other) -> {
			return original.getKey().toString().compareTo(other.getKey().toString());
		});
		for (Entry<Object, QueryableDatatype<?>> entry2 : entryList2) {
			encoder.add(entry2.getKey().toString(),entry2.getValue().toString());
		}



		final String toString = encoder.toString();
		return toString;
	}
}
