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
package nz.co.gregs.dbvolution.internal.h2;

import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public enum DateRepeatFunctions implements DBVFeature {

	/**
	 *
	 */
	CREATE("String", "Date original, Date compareTo", ""
			+ "import org.joda.time.Period;"
			+ "import java.util.*;",
			"		if (original==null||compareTo==null){return null;}\n"
			+ "		int years = original.getYear() - compareTo.getYear();\n"
			+ "		int months = original.getMonth() - compareTo.getMonth();\n"
			+ "		int days = original.getDate() - compareTo.getDate();\n"
			+ "		int hours = original.getHours() - compareTo.getHours();\n"
			+ "		int minutes = original.getMinutes() - compareTo.getMinutes();\n"
			+ "		int millis = (int) ((original.getTime() - ((original.getTime() / 1000) * 1000)) - (compareTo.getTime() - ((compareTo.getTime() / 1000) * 1000)));\n"
			+ "		double seconds = original.getSeconds() - compareTo.getSeconds()+(millis/1000.0);\n"
			+ "		String dateRepeatString = \"P\" + years + \"Y\" + months + \"M\" + days + \"D\" + hours + \"h\" + minutes + \"n\" + seconds + \"s\";\n"
			+ "		return dateRepeatString;"
			+ ""),
	/**
	 *
	 */
	EQUALS("boolean", "String original, String compareTo",
			"import org.joda.time.Period;"
			+ "import java.util.*;",
			"		if (original==null||compareTo==null){return false;}\n"
			+ "		String[] splitOriginal = original.split(\"[A-Za-z]\");\n"
			+ "		String[] splitCompareTo = compareTo.split(\"[A-Za-z]\");\n"
			+ "		for (int i = 1; i < splitCompareTo.length; i++) { // Start at 1 because the first split is empty\n"
			+ "			double intOriginal = Double.parseDouble(splitOriginal[i]);\n"
			+ "			double intCompareTo = Double.parseDouble(splitCompareTo[i]);\n"
			+ "			if (intOriginal > intCompareTo) {\n"
			+ "				return false;\n"
			+ "			}\n"
			+ "			if (intOriginal < intCompareTo) {\n"
			+ "				return false;\n"
			+ "			}\n"
			+ "		}\n"
			+ "		return true;\n"),
	/**
	 *
	 */
	LESSTHAN("boolean", "String original, String compareTo",
			"import org.joda.time.Period;"
			+ "import java.util.*;",
			"		if (original==null||compareTo==null){return false;}\n"
			+ "		String[] splitOriginal = original.split(\"[A-Za-z]\");\n"
			+ "		String[] splitCompareTo = compareTo.split(\"[A-Za-z]\");\n"
			+ "		for (int i = 1; i < splitCompareTo.length; i++) { // Start at 1 because the first split is empty\n"
			+ "			double intOriginal = Double.parseDouble(splitOriginal[i]);\n"
			+ "			double intCompareTo = Double.parseDouble(splitCompareTo[i]);\n"
			+ "			if (intOriginal > intCompareTo) {\n"
			+ "				return false;\n"
			+ "			}\n"
			+ "			if (intOriginal < intCompareTo) {\n"
			+ "				return true;\n"
			+ "			}\n"
			+ "		}\n"
			+ "		return false;\n"),
	/**
	 *
	 */
	LESSTHANEQUALS("boolean", "String original, String compareTo",
			"import org.joda.time.Period;"
			+ "import java.util.*;",
			"		if (original==null||compareTo==null){return false;}\n"
			+ "		String[] splitOriginal = original.split(\"[A-Za-z]\");\n"
			+ "		String[] splitCompareTo = compareTo.split(\"[A-Za-z]\");\n"
			+ "		for (int i = 1; i < splitCompareTo.length; i++) { // Start at 1 because the first split is empty\n"
			+ "			double intOriginal = Double.parseDouble(splitOriginal[i]);\n"
			+ "			double intCompareTo = Double.parseDouble(splitCompareTo[i]);\n"
			+ "			if (intOriginal > intCompareTo) {\n"
			+ "				return false;\n"
			+ "			}\n"
			+ "			if (intOriginal < intCompareTo) {\n"
			+ "				return true;\n"
			+ "			}\n"
			+ "		}\n"
			+ "		return true;\n"),
	/**
	 *
	 */
	GREATERTHAN("boolean", "String original, String compareTo",
			"import org.joda.time.Period;"
			+ "import java.util.*;",
			"		if (original==null||compareTo==null){return false;}\n"
			+ "		String[] splitOriginal = original.split(\"[A-Za-z]\");\n"
			+ "		String[] splitCompareTo = compareTo.split(\"[A-Za-z]\");\n"
			+ "		for (int i = 1; i < splitCompareTo.length; i++) { // Start at 1 because the first split is empty\n"
			+ "			double intOriginal = Double.parseDouble(splitOriginal[i]);\n"
			+ "			double intCompareTo = Double.parseDouble(splitCompareTo[i]);\n"
			+ "			if (intOriginal > intCompareTo) {\n"
			+ "				return true;\n"
			+ "			}\n"
			+ "			if (intOriginal < intCompareTo) {\n"
			+ "				return false;\n"
			+ "			}\n"
			+ "		}\n"
			+ "		return false;\n"),
	/**
	 *
	 */
	GREATERTHANEQUALS("boolean", "String original, String compareTo",
			"import org.joda.time.Period;"
			+ "import java.util.*;",
			"		if (original==null||compareTo==null){return false;}\n"
			+ "		String[] splitOriginal = original.split(\"[A-Za-z]\");\n"
			+ "		String[] splitCompareTo = compareTo.split(\"[A-Za-z]\");\n"
			+ "		for (int i = 1; i < splitCompareTo.length; i++) { // Start at 1 because the first split is empty\n"
			+ "			double intOriginal = Double.parseDouble(splitOriginal[i]);\n"
			+ "			double intCompareTo = Double.parseDouble(splitCompareTo[i]);\n"
			+ "			if (intOriginal > intCompareTo) {\n"
			+ "				return true;\n"
			+ "			}\n"
			+ "			if (intOriginal < intCompareTo) {\n"
			+ "				return false;\n"
			+ "			}\n"
			+ "		}\n"
			+ "		return true;\n"),
	/**
	 *
	 */
	DATEADDITION("Date", "Date original, String dateRepeatInput",
			"import org.joda.time.Period;"
			+ "import java.util.*;",
			"\n"
			+ "		if (original == null || dateRepeatInput == null || dateRepeatInput.length() == 0) {\n"
			+ "			return null;\n"
			+ "		}\n"
			+ "		String dateRepeatStr = dateRepeatInput.replaceAll(\"[^-.PYMDhns0-9]+\", \"\");\n"
			+ "		Calendar cal = new GregorianCalendar();\n"
			+ "		cal.setTime(original);\n"
			+ "		int years = Integer.parseInt(dateRepeatStr.replaceAll(\".*P([-0-9.]+)Y.*\", \"$1\"));\n"
			+ "		int months = Integer.parseInt(dateRepeatStr.replaceAll(\".*Y([-0-9.]+)M.*\", \"$1\"));\n"
			+ "		int days = Integer.parseInt(dateRepeatStr.replaceAll(\".*M([-0-9.]+)D.*\", \"$1\"));\n"
			+ "		int hours = Integer.parseInt(dateRepeatStr.replaceAll(\".*D([-0-9.]+)h.*\", \"$1\"));\n"
			+ "		int minutes = Integer.parseInt(dateRepeatStr.replaceAll(\".*h([-0-9.]+)n.*\", \"$1\"));\n"
			+ "		int seconds = Integer.valueOf(dateRepeatStr.replaceAll(\".*n([-0-9]+)[.]*([0-9])*s.*\", \"$1\"));\n"
			+ "		int millis = Integer.valueOf(dateRepeatStr.replaceAll(\".*n([-0-9]+)[.]*([0-9])*s.*\", \"$2\"));\n"
			+ "\n"
			+ "		cal.add(Calendar.YEAR, years);\n"
			+ "		cal.add(Calendar.MONTH, months);\n"
			+ "		cal.add(Calendar.DAY_OF_MONTH, days);\n"
			+ "		cal.add(Calendar.HOUR, hours);\n"
			+ "		cal.add(Calendar.MINUTE, minutes);\n"
			+ "		cal.add(Calendar.SECOND, seconds);\n"
			+ "		cal.add(Calendar.MILLISECOND, millis);\n"
			+ "		return cal.getTime();"
			+ ""),
	/**
	 *
	 */
	DATESUBTRACTION("Date", "Date original, String dateRepeatInput",
			"import org.joda.time.Period;"
			+ "import java.util.*;",
			"\n"
			+ "		if (original == null || dateRepeatInput == null || dateRepeatInput.length() == 0) {\n"
			+ "			return null;\n"
			+ "		}\n"
			+ "		String dateRepeatStr = dateRepeatInput.replaceAll(\"[^-.PYMDhns0-9]+\", \"\");\n"
			+ "		Calendar cal = new GregorianCalendar();\n"
			+ "		cal.setTime(original);\n"
			+ "		int years = Integer.parseInt(dateRepeatStr.replaceAll(\".*P([-0-9.]+)Y.*\", \"$1\"));\n"
			+ "		int months = Integer.parseInt(dateRepeatStr.replaceAll(\".*Y([-0-9.]+)M.*\", \"$1\"));\n"
			+ "		int days = Integer.parseInt(dateRepeatStr.replaceAll(\".*M([-0-9.]+)D.*\", \"$1\"));\n"
			+ "		int hours = Integer.parseInt(dateRepeatStr.replaceAll(\".*D([-0-9.]+)h.*\", \"$1\"));\n"
			+ "		int minutes = Integer.parseInt(dateRepeatStr.replaceAll(\".*h([-0-9.]+)n.*\", \"$1\"));\n"
			+ "		int seconds = Integer.valueOf(dateRepeatStr.replaceAll(\".*n([-0-9]+)[.]*([0-9])*s.*\", \"$1\"));\n"
			+ "		int millis = Integer.valueOf(dateRepeatStr.replaceAll(\".*n([-0-9]+)[.]*([0-9])*s.*\", \"$2\"));\n"
			+ "\n"
			+ "		cal.add(Calendar.YEAR, -1 * years);\n"
			+ "		cal.add(Calendar.MONTH, -1 * months);\n"
			+ "		cal.add(Calendar.DAY_OF_MONTH, -1 * days);\n"
			+ "		cal.add(Calendar.HOUR, -1 * hours);\n"
			+ "		cal.add(Calendar.MINUTE, -1 * minutes);\n"
			+ "		cal.add(Calendar.MILLISECOND, -1 * seconds);\n"
			+ "		return cal.getTime();"
			+ ""),
	/**
	 *
	 */
	YEAR_PART("Integer", "String dateRepeatStr", "", "NumberFormatException",
			"		if (dateRepeatStr==null||dateRepeatStr.length()==0){return null;}\n"
			+ "		return Integer.parseInt(dateRepeatStr.replaceAll(\".*P([-0-9.]+)Y.*\", \"$1\"));\n"),
	/**
	 *
	 */
	MONTH_PART("Integer", "String dateRepeatStr", "", "NumberFormatException",
			"		if (dateRepeatStr==null||dateRepeatStr.length()==0){return null;}\n"
			+ "		return Integer.parseInt(dateRepeatStr.replaceAll(\".*Y([-0-9.]+)M.*\", \"$1\"));\n"),
	/**
	 *
	 */
	DAY_PART("Integer", "String dateRepeatStr", "", "NumberFormatException",
			"		if (dateRepeatStr==null||dateRepeatStr.length()==0){return null;}\n"
			+ "		return Integer.parseInt(dateRepeatStr.replaceAll(\".*M([-0-9.]+)D.*\", \"$1\"));\n"),
	/**
	 *
	 */
	HOUR_PART("Integer", "String dateRepeatStr", "", "NumberFormatException",
			"		if (dateRepeatStr==null||dateRepeatStr.length()==0){return null;}\n"
			+ "		return Integer.parseInt(dateRepeatStr.replaceAll(\".*D([-0-9.]+)h.*\", \"$1\"));\n"),
	/**
	 *
	 */
	MINUTE_PART("Integer", "String dateRepeatStr", "", "NumberFormatException",
			"		if (dateRepeatStr==null||dateRepeatStr.length()==0){return null;}\n"
			+ "		return Integer.parseInt(dateRepeatStr.replaceAll(\".*h([-0-9.]+)n.*\", \"$1\"));\n"),
	/**
	 *
	 */
	SECOND_PART("Integer", "String dateRepeatStr", "", "NumberFormatException",
			"		if (dateRepeatStr==null||dateRepeatStr.length()==0){return null;}\n"
			+ "		return Double.valueOf(dateRepeatStr.replaceAll(\".*n([-0-9.]+)s.*\", \"$1\")).intValue();\n");

	private final String returnType;
	private final String parameters;
	private final String imports;
	private final String exceptions;
	private final String code;

	DateRepeatFunctions(String returnType, String parameters, String imports, String code) {
//		this.functionName = functionName;
		this.returnType = returnType;
		this.parameters = parameters;
		this.imports = imports;
		this.exceptions = "";
		this.code = code;
	}

	DateRepeatFunctions(String returnType, String parameters, String imports, String exceptions, String code) {
//		this.functionName = functionName;
		this.returnType = returnType;
		this.parameters = parameters;
		this.imports = imports;
		this.exceptions = exceptions;
		this.code = code;
	}

	@Override
	public String toString() {
		return "DBV_DATEREPEAT_" + name();
	}

	@Override
	public String alias() {
		return toString();
	}

	/**
	 *
	 * @param stmt
	 * @throws SQLException
	 */
	@Override
	public void add(Statement stmt) throws SQLException {
		try {
			stmt.execute("DROP ALIAS " + this + ";");
		} catch (SQLException sqlex) {
			;// Not an issue.
		}
		final String createFunctionStatement = "CREATE ALIAS IF NOT EXISTS " + this + " DETERMINISTIC AS $$ \n" + imports + "\n\n" + "@CODE " + returnType + " " + this + "(" + parameters + ") " + (exceptions.equals("") ? "" : "throws " + exceptions) + "{\n" + code + "} $$;";
		stmt.execute(createFunctionStatement);
	}
}
