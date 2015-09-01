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
package nz.co.gregs.dbvolution.reflection;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import nz.co.gregs.dbvolution.datatypes.DBBoolean;
import nz.co.gregs.dbvolution.datatypes.DBDate;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBIntegerEnum;
import nz.co.gregs.dbvolution.datatypes.DBNumber;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.DBStringEnum;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

/**
 *
 * @author gregorygraham
 */
public class DefaultEncodingInterpreter implements EncodingInterpreter {

	List<String> trueVals = new ArrayList<String>() {
		public static final long serialVersionUID = 1L;

		{
			this.add("y");
			this.add("yes");
			this.add("t");
			this.add("true");
			this.add("1");
		}
	};
	private String encodingSeparator = "&";
	private String tableAndPropertySeparator = "-";
	private String propertyAndValueSeparator = "=";

	@Override
	@SuppressWarnings("deprecation")
	public void setValue(QueryableDatatype qdt, String value) {
		if (qdt instanceof DBString) {
			DBString string = (DBString) qdt;
			string.permittedValues(value);
		} else if (qdt instanceof DBStringEnum) {
			DBStringEnum<?> string = (DBStringEnum<?>) qdt;
			string.permittedValues(value);
		} else if (qdt instanceof DBNumber) {
			DBNumber num = (DBNumber) qdt;
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
		} else if (qdt instanceof DBInteger) {
			DBInteger num = (DBInteger) qdt;
			if (value.contains("...")) {
				String[] split = value.split("\\.\\.\\.");
				Long startOfRange = split[0] == null || split[0].isEmpty() ? null : new Long(split[0]);
				Long endOfRange = split.length == 1 || split[1] == null || split[1].isEmpty() ? null : new Long(split[1]);
				num.permittedRangeInclusive(
						startOfRange,
						endOfRange);
			} else {
				num.permittedValues(new Double(value));
			}
		} else if (qdt instanceof DBIntegerEnum) {
			DBIntegerEnum<?> num = (DBIntegerEnum<?>) qdt;
			if (value.contains("...")) {
				String[] split = value.split("\\.\\.\\.");
				Long startOfRange = split[0] == null || split[0].isEmpty() ? null : new Long(split[0]);
				Long endOfRange = split.length == 1 || split[1] == null || split[1].isEmpty() ? null : new Long(split[1]);
				num.permittedRangeInclusive(
						startOfRange,
						endOfRange);
			} else {
				num.permittedValues(new Long(value));
			}
		} else if (qdt instanceof DBDate) {
			DBDate date = (DBDate) qdt;
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
		} else if (qdt instanceof DBBoolean) {
			DBBoolean field = (DBBoolean) qdt;
			field.permittedValues(trueVals.contains(value.toLowerCase()));
		}
	}

	@Override
	public String[] splitParameters(String encodedTablesPropertiesAndValues) {
		return encodedTablesPropertiesAndValues.split(getEncodingSeparator());
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
	 * @return the encodingSeparator
	 */
	public String getEncodingSeparator() {
		return encodingSeparator;
	}

	/**
	 * @param encodingSeparator the encodingSeparator to set
	 */
	public void setEncodingSeparator(String encodingSeparator) {
		this.encodingSeparator = encodingSeparator;
	}

	/**
	 * @return the tableAndPropertySeparator
	 */
	public String getTableAndPropertySeparator() {
		return tableAndPropertySeparator;
	}

	/**
	 * @param tableAndPropertySeparator the tableAndPropertySeparator to set
	 */
	public void setTableAndPropertySeparator(String tableAndPropertySeparator) {
		this.tableAndPropertySeparator = tableAndPropertySeparator;
	}

	/**
	 * @return the propertyAndValueSeparator
	 */
	public String getPropertyAndValueSeparator() {
		return propertyAndValueSeparator;
	}

	/**
	 * @param propertyAndValueSeparator the propertyAndValueSeparator to set
	 */
	public void setPropertyAndValueSeparator(String propertyAndValueSeparator) {
		this.propertyAndValueSeparator = propertyAndValueSeparator;
	}

}
