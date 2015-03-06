/*
 * Copyright 2013 greg.
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
package nz.co.gregs.dbvolution.databases;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.H2DBDefinition;
import nz.co.gregs.dbvolution.databases.supports.SupportsDateRepeatDatatypeFunctions;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.exceptions.UnableToCreateDatabaseConnectionException;
import nz.co.gregs.dbvolution.exceptions.UnableToFindJDBCDriver;

/**
 * Stores all the required functionality to use an H2 database.
 *
 * @author Gregory Graham
 */
public class H2DB extends DBDatabase implements SupportsDateRepeatDatatypeFunctions {

	/**
	 * Used to hold the database open
	 *
	 */
	protected Connection storedConnection;


	/**
	 * Creates a DBDatabase for a H2 database in the file supplied.
	 *
	 *
	 *
	 *
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @param file file
	 * @param username username
	 * @param password password
	 * @throws java.io.IOException java.io.IOException
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public H2DB(File file, String username, String password) throws IOException, SQLException {
		this("jdbc:h2:" + file.getCanonicalFile(), username, password);
	}

	/**
	 * Creates a DBDatabase for a H2 database.
	 *
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @param dataSource dataSource
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public H2DB(DataSource dataSource) throws SQLException {
		super(new H2DBDefinition(), dataSource);
		jamDatabaseConnectionOpen();
		addDateRepeatFunctions();
	}
	/**
	 * Creates a DBDatabase for a H2 database.
	 *
	 *
	 *
	 *
	 * 1 Database exceptions may be thrown
	 *
	 * @param jdbcURL jdbcURL
	 * @param username username
	 * @param password password
	 * @throws java.sql.SQLException java.sql.SQLException
	 */
	public H2DB(String jdbcURL, String username, String password) throws SQLException {
		super(new H2DBDefinition(), "org.h2.Driver", jdbcURL, username, password);
		jamDatabaseConnectionOpen();
		addDateRepeatFunctions();
	}

	private void addDateRepeatFunctions() throws UnableToFindJDBCDriver, UnableToCreateDatabaseConnectionException, SQLException {
		Connection connection = getConnection();
		Statement stmt = connection.createStatement();
		stmt.execute("CREATE DOMAIN IF NOT EXISTS DBV_DATEREPEAT AS VARCHAR(100); ");
		stmt.execute("CREATE ALIAS IF NOT EXISTS " + H2DBDefinition.DATEREPEAT_CREATION_FUNCTION + " DETERMINISTIC AS $$ \n"
				+ "import org.joda.time.Period;"
				+ "import java.util.*;"
				+ "@CODE String getDateRepeatString(Date original, Date compareTo) {\n"
				+ "		if (original==null||compareTo==null){return null;}\n"
				+ "		int years = original.getYear() - compareTo.getYear();\n"
				+ "		int months = original.getMonth() - compareTo.getMonth();\n"
				+ "		int days = original.getDate() - compareTo.getDate();\n"
				+ "		int hours = original.getHours() - compareTo.getHours();\n"
				+ "		int minutes = original.getMinutes() - compareTo.getMinutes();\n"
				+ "		int millis = (int) ((original.getTime() - ((original.getTime() / 1000) * 1000)) - (compareTo.getTime() - ((compareTo.getTime() / 1000) * 1000)));\n"
				+ "		double seconds = original.getSeconds() - compareTo.getSeconds()+(millis/1000.0);\n"
				+ "		String dateRepeatString = \"P\" + years + \"Y\" + months + \"M\" + days + \"D\" + hours + \"h\" + minutes + \"n\" + seconds + \"s\";\n"
				+ "		return dateRepeatString;"
				+ "	} $$;");
		stmt.execute("CREATE ALIAS IF NOT EXISTS " + H2DBDefinition.DATEREPEAT_DATEADDITION_FUNCTION + " DETERMINISTIC AS $$ \n"
				+ "import org.joda.time.Period;"
				+ "import java.util.*;"
				+ "import java.lang.*;"
				+ "@CODE Date addDateAndDateRepeatString(Date original, String dateRepeatStr) {\n"
				+ "		if (original==null||dateRepeatStr==null||dateRepeatStr.length()==0||original.toString().length()==0||original.getTime()==0){return null;}else{\n"
				+ "		Calendar cal = new GregorianCalendar();\n"
				+ "		try{cal.setTime(original);}catch(Exception except){return null;}\n"
				+ "		int years = Integer.parseInt(dateRepeatStr.replaceAll(\".*P([-0-9.]+)Y.*\", \"$1\"));\n"
				+ "		int months = Integer.parseInt(dateRepeatStr.replaceAll(\".*Y([-0-9.]+)M.*\", \"$1\"));\n"
				+ "		int days = Integer.parseInt(dateRepeatStr.replaceAll(\".*M([-0-9.]+)D.*\", \"$1\"));\n"
				+ "		int hours = Integer.parseInt(dateRepeatStr.replaceAll(\".*D([-0-9.]+)h.*\", \"$1\"));\n"
				+ "		int minutes = Integer.parseInt(dateRepeatStr.replaceAll(\".*h([-0-9.]+)n.*\", \"$1\"));\n"
				+ "		int seconds = Integer.parseInt(dateRepeatStr.replaceAll(\".*m([-0-9.]+)s.*\", \"$1\"));\n"
				+ "		final Double secondsDouble = Double.parseDouble(dateRepeatStr.replaceAll(\".*n([-0-9.]+)s.*\", \"$1\"));\n"
				+ "		final int secondsInt = secondsDouble.intValue();\n"
				+ "		cal.add(Calendar.YEAR, years);\n"
				+ "		cal.add(Calendar.MONTH, months);\n"
				+ "		cal.add(Calendar.DAY_OF_MONTH, days);\n"
				+ "		cal.add(Calendar.HOUR, hours);\n"
				+ "		cal.add(Calendar.MINUTE, minutes);\n"
				+ "		cal.add(Calendar.SECOND, seconds);\n"
				+ "		return cal.getTime();}\n"
				+ "	} $$;");
		stmt.execute("CREATE ALIAS IF NOT EXISTS " + H2DBDefinition.DATEREPEAT_DATESUBTRACTION_FUNCTION + " DETERMINISTIC AS $$ \n"
				+ "import org.joda.time.Period;"
				+ "import java.util.*;"
				+ "@CODE Date subtractDateAndDateRepeatString(Date original, String dateRepeatInput) {\n"
				+ "		if (original == null || dateRepeatInput == null || dateRepeatInput.length() == 0) {\n"
				+ "			return null;\n"
				+ "		}\n"
				+ "		String dateRepeatStr = dateRepeatInput.replaceAll(\"[^-.PYMDhns0-9]+\", \"\");\n"
				+ "		Calendar cal = new GregorianCalendar();\n"
				+ "		cal.setTime(original);\n"
				+ "		int years = getYearPart(dateRepeatStr);\n"
				+ "		int months = getMonthPart(dateRepeatStr);\n"
				+ "		int days = getDayPart(dateRepeatStr);\n"
				+ "		int hours = getHourPart(dateRepeatStr);\n"
				+ "		int minutes = getMinutePart(dateRepeatStr);\n"
				+ "		int seconds = getSecondPart(dateRepeatStr);\n"
				+ "\n"
				+ "		cal.add(Calendar.YEAR, -1 * years);\n"
				+ "		cal.add(Calendar.MONTH, -1 * months);\n"
				+ "		cal.add(Calendar.DAY_OF_MONTH, -1 * days);\n"
				+ "		cal.add(Calendar.HOUR, -1 * hours);\n"
				+ "		cal.add(Calendar.MINUTE, -1 * minutes);\n"
				+ "		cal.add(Calendar.SECOND, -1 * seconds);\n"
				+ "		return cal.getTime();"
				+ "	} \n"
				+ "\n"
				+ "	public static Integer getMillisecondPart(String dateRepeatStr) throws NumberFormatException {\n"
				+ "		if (dateRepeatStr == null || dateRepeatStr.length() == 0) {\n"
				+ "			return null;\n"
				+ "		}\n"
				+ "		final Double secondsDouble = Double.parseDouble(dateRepeatStr.replaceAll(\".*n([-0-9.]+)s.*\", \"$1\"));\n"
				+ "		final int secondsInt = secondsDouble.intValue();\n"
				+ "		final int millis = (int) ((secondsDouble * 1000.0) - (secondsInt * 1000));\n"
				+ "		return millis;\n"
				+ "	}\n"
				+ "\n"
				+ "	public static Integer getSecondPart(String dateRepeatStr) throws NumberFormatException {\n"
				+ "		if (dateRepeatStr == null || dateRepeatStr.length() == 0) {\n"
				+ "			return null;\n"
				+ "		}\n"
				+ "		return Double.valueOf(dateRepeatStr.replaceAll(\".*n([-0-9.]+)s.*\", \"$1\")).intValue();\n"
				+ "	}\n"
				+ "\n"
				+ "	public static Integer getMinutePart(String dateRepeatStr) throws NumberFormatException {\n"
				+ "		if (dateRepeatStr == null || dateRepeatStr.length() == 0) {\n"
				+ "			return null;\n"
				+ "		}\n"
				+ "		return Integer.parseInt(dateRepeatStr.replaceAll(\".*h([-0-9.]+)n.*\", \"$1\"));\n"
				+ "	}\n"
				+ "\n"
				+ "	public static Integer getHourPart(String dateRepeatStr) throws NumberFormatException {\n"
				+ "		if (dateRepeatStr == null || dateRepeatStr.length() == 0) {\n"
				+ "			return null;\n"
				+ "		}\n"
				+ "		return Integer.parseInt(dateRepeatStr.replaceAll(\".*D([-0-9.]+)h.*\", \"$1\"));\n"
				+ "	}\n"
				+ "\n"
				+ "	public static Integer getDayPart(String dateRepeatStr) throws NumberFormatException {\n"
				+ "		if (dateRepeatStr == null || dateRepeatStr.length() == 0) {\n"
				+ "			return null;\n"
				+ "		}\n"
				+ "		return Integer.parseInt(dateRepeatStr.replaceAll(\".*M([-0-9.]+)D.*\", \"$1\"));\n"
				+ "	}\n"
				+ "\n"
				+ "	public static Integer getMonthPart(String dateRepeatStr) throws NumberFormatException {\n"
				+ "		if (dateRepeatStr == null || dateRepeatStr.length() == 0) {\n"
				+ "			return null;\n"
				+ "		}\n"
				+ "		return Integer.parseInt(dateRepeatStr.replaceAll(\".*Y([-0-9.]+)M.*\", \"$1\"));\n"
				+ "	}\n"
				+ "\n"
				+ "	public static Integer getYearPart(String dateRepeatStr) throws NumberFormatException {\n"
				+ "		if (dateRepeatStr == null || dateRepeatStr.length() == 0) {\n"
				+ "			return null;\n"
				+ "		}\n"
				+ "		return Integer.parseInt(dateRepeatStr.replaceAll(\".*P([-0-9.]+)Y.*\", \"$1\"));\n"
				+ "	}$$;");
		stmt.execute("CREATE ALIAS IF NOT EXISTS " + H2DBDefinition.DATEREPEAT_EQUALS_FUNCTION + " DETERMINISTIC AS $$ \n"
				+ "import org.joda.time.Period;"
				+ "import java.util.*;"
				+ "@CODE boolean isEqualTo(String original, String compareTo) {\n"
				+ "		if (original==null||compareTo==null){return false;}\n"
				+ "		String[] splitOriginal = original.split(\"[A-Za-z]\");\n"
				+ "		String[] splitCompareTo = compareTo.split(\"[A-Za-z]\");\n"
				+ "		for (int i = 1; i < splitCompareTo.length; i++) { // Start at 1 because the first split is empty\n"
				+ "			System.out.println(\"SPLITORIGINAL \"+i+\": \"+splitOriginal[i]);\n"
				+ "			double intOriginal = Double.parseDouble(splitOriginal[i]);\n"
				+ "			double intCompareTo = Double.parseDouble(splitCompareTo[i]);\n"
				+ "			if (intOriginal > intCompareTo) {\n"
				+ "				return false;\n"
				+ "			}\n"
				+ "			if (intOriginal < intCompareTo) {\n"
				+ "				return false;\n"
				+ "			}\n"
				+ "		}\n"
				+ "		return true;\n"
				+ "	} $$;");
		stmt.execute("CREATE ALIAS IF NOT EXISTS " + H2DBDefinition.DATEREPEAT_GREATERTHANEQUALS_FUNCTION + " DETERMINISTIC AS $$ \n"
				+ "import org.joda.time.Period;"
				+ "import java.util.*;"
				+ "@CODE boolean isEqualTo(String original, String compareTo) {\n"
				+ "		if (original==null||compareTo==null){return false;}\n"
				+ "		String[] splitOriginal = original.split(\"[A-Za-z]\");\n"
				+ "		String[] splitCompareTo = compareTo.split(\"[A-Za-z]\");\n"
				+ "		for (int i = 1; i < splitCompareTo.length; i++) { // Start at 1 because the first split is empty\n"
				+ "			System.out.println(\"SPLITORIGINAL \"+i+\": \"+splitOriginal[i]);\n"
				+ "			double intOriginal = Double.parseDouble(splitOriginal[i]);\n"
				+ "			double intCompareTo = Double.parseDouble(splitCompareTo[i]);\n"
				+ "			if (intOriginal > intCompareTo) {\n"
				+ "				return true;\n"
				+ "			}\n"
				+ "			if (intOriginal < intCompareTo) {\n"
				+ "				return false;\n"
				+ "			}\n"
				+ "		}\n"
				+ "		return true;\n"
				+ "	} $$;");
		stmt.execute("CREATE ALIAS IF NOT EXISTS " + H2DBDefinition.DATEREPEAT_GREATERTHAN_FUNCTION + " DETERMINISTIC AS $$ \n"
				+ "import org.joda.time.Period;"
				+ "import java.util.*;"
				+ "@CODE boolean isEqualTo(String original, String compareTo) {\n"
				+ "		if (original==null||compareTo==null){return false;}\n"
				+ "		String[] splitOriginal = original.split(\"[A-Za-z]\");\n"
				+ "		String[] splitCompareTo = compareTo.split(\"[A-Za-z]\");\n"
				+ "		for (int i = 1; i < splitCompareTo.length; i++) { // Start at 1 because the first split is empty\n"
				+ "			System.out.println(\"SPLITORIGINAL \"+i+\": \"+splitOriginal[i]);\n"
				+ "			double intOriginal = Double.parseDouble(splitOriginal[i]);\n"
				+ "			double intCompareTo = Double.parseDouble(splitCompareTo[i]);\n"
				+ "			if (intOriginal > intCompareTo) {\n"
				+ "				return true;\n"
				+ "			}\n"
				+ "			if (intOriginal < intCompareTo) {\n"
				+ "				return false;\n"
				+ "			}\n"
				+ "		}\n"
				+ "		return false;\n"
				+ "	} $$;");
		stmt.execute("CREATE ALIAS IF NOT EXISTS " + H2DBDefinition.DATEREPEAT_LESSTHANEQUALS_FUNCTION + " DETERMINISTIC AS $$ \n"
				+ "import org.joda.time.Period;"
				+ "import java.util.*;"
				+ "@CODE boolean isEqualTo(String original, String compareTo) {\n"
				+ "		if (original==null||compareTo==null){return false;}\n"
				+ "		String[] splitOriginal = original.split(\"[A-Za-z]\");\n"
				+ "		String[] splitCompareTo = compareTo.split(\"[A-Za-z]\");\n"
				+ "		for (int i = 1; i < splitCompareTo.length; i++) { // Start at 1 because the first split is empty\n"
				+ "			System.out.println(\"SPLITORIGINAL \"+i+\": \"+splitOriginal[i]);\n"
				+ "			double intOriginal = Double.parseDouble(splitOriginal[i]);\n"
				+ "			double intCompareTo = Double.parseDouble(splitCompareTo[i]);\n"
				+ "			if (intOriginal > intCompareTo) {\n"
				+ "				return false;\n"
				+ "			}\n"
				+ "			if (intOriginal < intCompareTo) {\n"
				+ "				return true;\n"
				+ "			}\n"
				+ "		}\n"
				+ "		return true;\n"
				+ "	} $$;");
		stmt.execute("CREATE ALIAS IF NOT EXISTS " + H2DBDefinition.DATEREPEAT_LESSTHAN_FUNCTION + " DETERMINISTIC AS $$ \n"
				+ "import org.joda.time.Period;"
				+ "import java.util.*;"
				+ "@CODE boolean isEqualTo(String original, String compareTo) {\n"
				+ "		if (original==null||compareTo==null){return false;}\n"
				+ "		String[] splitOriginal = original.split(\"[A-Za-z]\");\n"
				+ "		String[] splitCompareTo = compareTo.split(\"[A-Za-z]\");\n"
				+ "		for (int i = 1; i < splitCompareTo.length; i++) { // Start at 1 because the first split is empty\n"
				+ "			System.out.println(\"SPLITORIGINAL \"+i+\": \"+splitOriginal[i]);\n"
				+ "			double intOriginal = Double.parseDouble(splitOriginal[i]);\n"
				+ "			double intCompareTo = Double.parseDouble(splitCompareTo[i]);\n"
				+ "			if (intOriginal > intCompareTo) {\n"
				+ "				return false;\n"
				+ "			}\n"
				+ "			if (intOriginal < intCompareTo) {\n"
				+ "				return true;\n"
				+ "			}\n"
				+ "		}\n"
				+ "		return false;\n"
				+ "	} $$;");

		stmt.execute("CREATE ALIAS IF NOT EXISTS " + H2DBDefinition.DATEREPEAT_YEAR_PART_FUNCTION + " DETERMINISTIC AS $$ \n"
				+ "Integer getYearPart(String dateRepeatStr) throws NumberFormatException {\n"
				+ "		if (dateRepeatStr==null||dateRepeatStr.length()==0){return null;}\n"
				+ "		return Integer.parseInt(dateRepeatStr.replaceAll(\".*P([-0-9.]+)Y.*\", \"$1\"));\n"
				+ "	} $$;");
		stmt.execute("CREATE ALIAS IF NOT EXISTS " + H2DBDefinition.DATEREPEAT_MONTH_PART_FUNCTION + " DETERMINISTIC AS $$ \n"
				+ "Integer getMonthPart(String dateRepeatStr) throws NumberFormatException {\n"
				+ "		if (dateRepeatStr==null||dateRepeatStr.length()==0){return null;}\n"
				+ "		return Integer.parseInt(dateRepeatStr.replaceAll(\".*Y([-0-9.]+)M.*\", \"$1\"));\n"
				+ "	} $$;");
		stmt.execute("CREATE ALIAS IF NOT EXISTS " + H2DBDefinition.DATEREPEAT_DAY_PART_FUNCTION + " DETERMINISTIC AS $$ \n"
				+ "Integer getDayPart(String dateRepeatStr) throws NumberFormatException {\n"
				+ "		if (dateRepeatStr==null||dateRepeatStr.length()==0){return null;}\n"
				+ "		return Integer.parseInt(dateRepeatStr.replaceAll(\".*M([-0-9.]+)D.*\", \"$1\"));\n"
				+ "	} $$;");
		stmt.execute("CREATE ALIAS IF NOT EXISTS " + H2DBDefinition.DATEREPEAT_HOUR_PART_FUNCTION + " DETERMINISTIC AS $$ \n"
				+ "Integer getHourPart(String dateRepeatStr) throws NumberFormatException {\n"
				+ "		if (dateRepeatStr==null||dateRepeatStr.length()==0){return null;}\n"
				+ "		return Integer.parseInt(dateRepeatStr.replaceAll(\".*D([-0-9.]+)h.*\", \"$1\"));\n"
				+ "	} $$;");
		stmt.execute("CREATE ALIAS IF NOT EXISTS " + H2DBDefinition.DATEREPEAT_MINUTE_PART_FUNCTION + " DETERMINISTIC AS $$ \n"
				+ "Integer getMinutePart(String dateRepeatStr) throws NumberFormatException {\n"
				+ "		if (dateRepeatStr==null||dateRepeatStr.length()==0){return null;}\n"
				+ "		return Integer.parseInt(dateRepeatStr.replaceAll(\".*h([-0-9.]+)n.*\", \"$1\"));\n"
				+ "	} $$;");
		stmt.execute("CREATE ALIAS IF NOT EXISTS " + H2DBDefinition.DATEREPEAT_SECOND_PART_FUNCTION + " DETERMINISTIC AS $$ \n"
				+ "Integer getSecondPart(String dateRepeatStr) throws NumberFormatException {\n"
				+ "		if (dateRepeatStr==null||dateRepeatStr.length()==0){return null;}\n"
				+ "		return Double.valueOf(dateRepeatStr.replaceAll(\".*n([-0-9.]+)s.*\", \"$1\")).intValue();\n"
				+ "	} $$;");
	}

	private void jamDatabaseConnectionOpen() throws DBRuntimeException, SQLException {
		this.storedConnection = getConnection();
		this.storedConnection.createStatement();
	}

	@Override
	public boolean supportsFullOuterJoinNatively() {
		return false;
	}

	/**
	 * Clones the DBDatabase
	 *
	 * @return a clone of the database.
	 * @throws java.lang.CloneNotSupportedException
	 * java.lang.CloneNotSupportedException
	 *
	 */
	@Override
	public H2DB clone() throws CloneNotSupportedException {
		return (H2DB) super.clone(); 
	}

}
