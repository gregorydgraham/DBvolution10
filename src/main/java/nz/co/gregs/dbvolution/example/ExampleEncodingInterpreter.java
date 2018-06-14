/*
 * Copyright 2015 gregorygraham.
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
package nz.co.gregs.dbvolution.example;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBIntegerEnum;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.DBStringEnum;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import nz.co.gregs.dbvolution.reflection.EncodingInterpreter;

/**
 * A simple example implementation of {@link EncodingInterpreter} that uses
 * "&amp;", "-", and "=" to separate the encoded parts.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class ExampleEncodingInterpreter implements EncodingInterpreter {

	private static final List<String> TRUE_VALUES = new ArrayList<String>() {
		public static final long serialVersionUID = 1L;

		{
			this.add("y");
			this.add("yes");
			this.add("t");
			this.add("true");
			this.add("1");
		}
	};
	private static final String ENCODING_SEPARATOR = "&";
	private static final String TABLE_AND_PROPERTY_SEPARATOR = "-";
	private static final String PROPERTY_AND_VALUE_SEPARATOR = "=";

	/**
	 * For all rows in the collection of DBRows encode the class and any
	 * {@link QueryableDatatype#hasBeenSet() set properties}.
	 *
	 * @param rows all the defined rows to be encoded.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an encoded string of the rows.
	 */
	@Override
	public String encode(DBRow... rows) {
		StringBuilder buf = new StringBuilder();
		String parameterSep = "";
		String actualParameterSeparator = getParameterSeparator();
		Set<Class<? extends DBRow>> addedAlready = new HashSet<>();
		for (DBRow row : rows) {
			Class<? extends DBRow> rowClass = row.getClass();
			List<PropertyWrapper> props = row.getColumnPropertyWrappers();
			for (PropertyWrapper prop : props) {
				QueryableDatatype<?> qdt = prop.getQueryableDatatype();
				if (qdt.hasBeenSet()) {
					String stringValue = qdt.stringValue();
					if (qdt instanceof DBDate) {
						DBDate dateQDT = (DBDate) qdt;
						Date dateValue = dateQDT.dateValue();
						stringValue = (new SimpleDateFormat("MMM dd HH:mm:ss yyyy")).format(dateValue);
					} else if (qdt instanceof DBNumber) {
						DBNumber numberQDT = (DBNumber) qdt;
						stringValue = "" + numberQDT.intValue();
					}
					buf.append(parameterSep)
							.append(rowClass.getCanonicalName())
							.append(getTableAndPropertySeparator())
							.append(prop.javaName())
							.append(getPropertyAndValueSeparator())
							.append(stringValue);
					parameterSep = actualParameterSeparator;
				} else {
					if (!row.getDefined() && !addedAlready.contains(rowClass)) {
						buf.append(parameterSep)
								.append(rowClass.getCanonicalName());
						addedAlready.add(rowClass);
						parameterSep = actualParameterSeparator;
					}
				}
			}
		}
		return buf.toString();
	}

	/**
	 * For all rows in the collection of DBRows encode the class and any
	 * {@link QueryableDatatype#hasBeenSet() set properties}.
	 *
	 * @param rows all the defined rows to be encoded.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an encoded string of the rows.
	 */
	public String encode(Collection<DBRow> rows) {
		return encode(rows.toArray(new DBRow[]{}));
	}

	/**
	 * For all rows in the collection of DBRows encode the class and any
	 * {@link QueryableDatatype#hasBeenSet() set properties}.
	 *
	 * @param queryRow all the defined rows to be encoded.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an encoded string of the rows.
	 */
	public String encode(DBQueryRow queryRow) {
		return encode(queryRow.values().toArray(new DBRow[]{}));
	}

	/**
	 * For all rows in the collection of DBRows encode the class and any
	 * {@link QueryableDatatype#hasBeenSet() set properties}.
	 *
	 * @param queryRows all the defined rows to be encoded.
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return an encoded string of the rows.
	 */
	public String encode(List<DBQueryRow> queryRows) {
		List<DBRow> rows = new ArrayList<>();
		for (DBQueryRow queryRow : queryRows) {
			rows.addAll(queryRow.values());
		}
		return encode(rows.toArray(new DBRow[]{}));
	}

	@Override
	@SuppressWarnings("deprecation")
	public void setValue(QueryableDatatype<?> qdt, String value) {
		if (qdt instanceof DBString) {
			DBString string = (DBString) qdt;
			decodeValue(value, string);
		} else if (qdt instanceof DBStringEnum) {
			DBStringEnum<?> string = (DBStringEnum<?>) qdt;
			decodeValue(value, string);
		} else if (qdt instanceof DBNumber) {
			DBNumber num = (DBNumber) qdt;
			decodeValue(value, num);
		} else if (qdt instanceof DBInteger) {
			DBInteger num = (DBInteger) qdt;
			decodeValue(value, num);
		} else if (qdt instanceof DBIntegerEnum) {
			DBIntegerEnum<?> num = (DBIntegerEnum<?>) qdt;
			decodeValue(value, num);
		} else if (qdt instanceof DBDate) {
			DBDate date = (DBDate) qdt;
			decodeValue(value, date);
		} else if (qdt instanceof DBBoolean) {
			DBBoolean field = (DBBoolean) qdt;
			decodeValue(value, field);
		}
	}

	@Override
	public void decodeValue(String value, DBBoolean field) {
		field.permittedValues(TRUE_VALUES.contains(value.toLowerCase()));
	}

	@Override
	@SuppressWarnings("deprecation")
	public void decodeValue(String value, DBDate date) {
		if (value.contains("...")) {
			String[] split = value.split("\\.\\.\\.");
			Date startOfRange = split[0] == null || split[0].isEmpty() ? null : new Date(split[0]);
			Date endOfRange = split.length == 1 || split[1] == null || split[1].isEmpty() ? null : new Date(split[1]);
			date.permittedRangeInclusive(
					startOfRange,
					endOfRange);
		} else {
			date.permittedValues(new Date(value));
		}
	}

	@Override
	public void decodeValue(String value, DBIntegerEnum<?> num) throws NumberFormatException {
		if (value.contains("...")) {
			String[] split = value.split("\\.\\.\\.");
			Long startOfRange = split[0] == null || split[0].isEmpty() ? null : Long.valueOf(split[0]);
			Long endOfRange = split.length == 1 || split[1] == null || split[1].isEmpty() ? null : Long.valueOf(split[1]);
			num.permittedRangeInclusive(
					startOfRange,
					endOfRange);
		} else {
			num.permittedValues(Long.valueOf(value));
		}
	}

	@Override
	public void decodeValue(String value, DBNumber num) throws NumberFormatException {
		if (value.contains("...")) {
			String[] split = value.split("\\.\\.\\.");
			Double startOfRange = split[0] == null || split[0].isEmpty() ? null : new Double(split[0]);
			Double endOfRange = split.length == 1 || split[1] == null || split[1].isEmpty() ? null : new Double(split[1]);
			num.permittedRangeInclusive(
					startOfRange,
					endOfRange);
		} else {
			num.permittedValues(new Double(value));
		}
	}

	@Override
	public void decodeValue(String value, DBStringEnum<?> string) {
		string.permittedValues(value);
	}

	@Override
	public void decodeValue(String value, DBString string) {
		string.permittedValues(value);
	}

	@Override
	public void decodeValue(String value, DBInteger num) throws NumberFormatException {
		if (value.contains("...")) {
			String[] split = value.split("\\.\\.\\.");
			Long startOfRange = split[0] == null || split[0].isEmpty() ? null : Long.valueOf(split[0]);
			Long endOfRange = split.length == 1 || split[1] == null || split[1].isEmpty() ? null : Long.valueOf(split[1]);
			num.permittedRangeInclusive(
					startOfRange,
					endOfRange);
		} else {
			num.permittedValues(new Double(value));
		}
	}

	@Override
	public String[] splitParameters(String encodedTablesPropertiesAndValues) {
		return encodedTablesPropertiesAndValues.split(getParameterSeparator());
	}

	@Override
	public String getDBRowClassName(String parameter) {
		return parameter.split(getTableAndPropertySeparator())[0];
	}

	@Override
	public String getPropertyName(String parameter) {
		final String[] split = parameter.split(getTableAndPropertySeparator());
		if (split.length > 1) {
			return split[1].split(getPropertyAndValueSeparator())[0];
		}
		return null;
	}

	@Override
	public String getPropertyValue(String parameter) {
		final String[] split = parameter.split(getTableAndPropertySeparator());
		if (split.length > 1) {
			final String[] split1 = split[1].split(getPropertyAndValueSeparator());
			if (split1.length > 1) {
				return split1[1];
			}
		}
		return null;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the encodingSeparator
	 */
	public String getParameterSeparator() {
		return ENCODING_SEPARATOR;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the TABLE_AND_PROPERTY_SEPARATOR
	 */
	public String getTableAndPropertySeparator() {
		return TABLE_AND_PROPERTY_SEPARATOR;
	}

	/**
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the PROPERTY_AND_VALUE_SEPARATOR
	 */
	public String getPropertyAndValueSeparator() {
		return PROPERTY_AND_VALUE_SEPARATOR;
	}
}
