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
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Contains all the instances of DBRow that are associated with one line of a
 * DBQuery request.
 *
 * <p>
 * DBvolution is available on <a
 * href="https://sourceforge.net/projects/dbvolution/">SourceForge</a> complete
 * with <a href="https://sourceforge.net/p/dbvolution/blog/">BLOG</a>
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
public class DBQueryRow extends HashMap<Class<?>, DBRow> {

	private static final long serialVersionUID = 1;
	private final Map<Object, QueryableDatatype> expressionColumnValues = new LinkedHashMap<Object, QueryableDatatype>();

	/**
	 * Returns the instance of exemplar contained within this DBQueryRow.
	 *
	 * <p>
	 * Finds the instance of the class supplied that is relevant to the DBRow
	 * and returns it.
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
	 * @param <E>
	 * @param exemplar
	 * @return the instance of exemplar that is in the DBQueryRow instance
	 */
	@SuppressWarnings("unchecked")
	public <E extends DBRow> E get(E exemplar) {
		return (E) get(exemplar.getClass());
	}

	/**
	 * Print the specified columns to the specified PrintStream as one line.
	 *
	 * @param ps
	 * @param columns
	 */
	public void print(PrintStream ps, QueryableDatatype... columns) {
		for (QueryableDatatype qdt : columns) {
			ps.print("" + qdt + " ");
		}
		ps.println();
	}

	/**
	 * Print the all columns to the specified PrintStream as one line.
	 *
	 * @param ps
	 */
	public void print(PrintStream ps) {
		for (DBRow row : values()) {
			ps.print("" + row);
		}
	}

	void addExpressionColumnValue(Object key, QueryableDatatype expressionQDT) {
		expressionColumnValues.put(key, expressionQDT);
	}

	QueryableDatatype getExpressionColumnValue(Object key) {
		return expressionColumnValues.get(key);
	}

	public List<String> getFieldNames() throws SecurityException {
		List<String> returnList = new ArrayList<String>();
		Set<Class<?>> keySet = this.keySet();
		for (Class<?> keySet1 : keySet) {
			Field[] fields = keySet1.getFields();
			for (Field field : fields) {
				returnList.add(field.getName());
			}
		}
		return returnList;
	}

	public List<String> getFieldValues() throws IllegalArgumentException, IllegalAccessException {
		List<String> returnList = new ArrayList<String>();
		Set<Class<?>> keySet = this.keySet();
		for (Class<?> keySet1 : keySet) {
			DBRow row = this.get(keySet1);
			Field[] fields = keySet1.getFields();
			for (Field field : fields) {
				String value = "";
				if (row != null) {
					value = field.get(row).toString().trim();
				}
				returnList.add(value);
			}
		}
		return returnList;
	}

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

	public String toSeparatedLine(String separatorToUseBetweenValues) throws IllegalArgumentException, IllegalAccessException {
		StringBuilder returnStr = new StringBuilder();
		String separator = "";
		List<String> fieldValues = this.getFieldValues();
		for (String fieldValue : fieldValues) {
			returnStr.append(separator).append("\"").append(fieldValue.replaceAll("\"", "\"\"")).append("\"");
			separator = separatorToUseBetweenValues;
		}
		return returnStr.toString();
	}

	public String toCSVHeader() {
		return toSeparatedHeader(",");
	}

	public String toCSVLine() throws IllegalArgumentException, IllegalAccessException {
		return toSeparatedLine(",");
	}

	public String toTabbedHeader() {
		return toSeparatedHeader("\t");
	}

	public String toTabbedLine() throws IllegalArgumentException, IllegalAccessException {
		return toSeparatedLine("\t");
	}
}
