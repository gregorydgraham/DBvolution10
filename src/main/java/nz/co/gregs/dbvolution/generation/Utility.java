/*
 * Copyright 2023 Gregory Graham.
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
package nz.co.gregs.dbvolution.generation;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.exceptions.NoAvailableDatabaseException;
import nz.co.gregs.dbvolution.exceptions.UnknownJavaSQLTypeException;

/**
 *
 * @author gregorygraham
 */
public class Utility {

	private Utility() {

	}

	public static final String[] JAVA_RESERVED_WORDS_ARRAY = new String[]{"null", "true", "false", "abstract", "continue", "for", "new", "switch", "assert", "default", "goto", "package", "synchronized", "boolean", "do", "if", "private", "this", "break", "double", "implements", "", "protected", "throw", "byte", "else", "import", "public", "throws", "case", "enum", "instanceof", "return", "transient", "catch", "extends", "int", "short", "try", "char", "final", "interface", "static", "", "void", "class", "finally", "long", "strictfp", "volatile", "const", "float", "native", "super", "while"};
	public static final List<String> JAVA_RESERVED_WORDS = Arrays.asList(JAVA_RESERVED_WORDS_ARRAY);

	/**
	 *
	 * Returns a string of the appropriate QueryableDatatype for the specified
	 * SQLType
	 *
	 *
	 *
	 *
	 *
	 * @param database
	 * @param typeName
	 * @param columnType
	 * @param precision
	 * @param trimCharColumns
	 * @return a string of the appropriate QueryableDatatype for the specified
	 * SQLType
	 */
	public static Class<? extends Object> getQDTClassOfSQLType(DBDatabase database, String typeName, int columnType, int precision, Boolean trimCharColumns) throws UnknownJavaSQLTypeException {

		Class<? extends Object> value;
		switch (columnType) {
			case Types.BIT:
				if (precision == 1) {
					value = checkForSimulatedTypesOrUseDefault(database, typeName, DBBoolean.class);
				} else {
					value = checkForSimulatedTypesOrUseDefault(database, typeName, DBLargeBinary.class);
				}
				break;
			case Types.TINYINT:
			case Types.INTEGER:
			case Types.BIGINT:
			case Types.BOOLEAN:
			case Types.ROWID:
			case Types.SMALLINT:
				if (precision == 1) {
					value = checkForSimulatedTypesOrUseDefault(database, typeName, DBBoolean.class);
				} else {
					value = checkForSimulatedTypesOrUseDefault(database, typeName, DBInteger.class);
				}
				break;
			case Types.DECIMAL:
			case Types.DOUBLE:
			case Types.FLOAT:
			case Types.NUMERIC:
			case Types.REAL:
				value =checkForSimulatedTypesOrUseDefault(database, typeName, DBNumber.class);
				break;
			case Types.CHAR:
			case Types.NCHAR:
				if (trimCharColumns) {
					value = checkForSimulatedTypesOrUseDefault(database, typeName, DBStringTrimmed.class);
				} else {
					value = checkForSimulatedTypesOrUseDefault(database, typeName, DBString.class);
				}
				break;
			case Types.VARCHAR:
			case Types.NVARCHAR:
			case Types.LONGNVARCHAR:
			case Types.LONGVARCHAR:
			value = checkForSimulatedTypesOrUseDefault(database, typeName, DBString.class);
			break;
			case Types.DATE:
			case Types.TIME:
			case Types.TIMESTAMP:
				value = checkForSimulatedTypesOrUseDefault(database, typeName, DBDate.class);
				break;
			case Types.OTHER:
			value = checkForSimulatedTypesOrUseDefault(database, typeName, DBJavaObject.class);
			break;
			case Types.JAVA_OBJECT:
				value = checkForSimulatedTypesOrUseDefault(database, typeName, DBJavaObject.class);
				break;
			case Types.CLOB:
			case Types.NCLOB:
				value = checkForSimulatedTypesOrUseDefault(database, typeName, DBLargeText.class);
				break;
			case Types.BINARY:
			case Types.VARBINARY:
			case Types.LONGVARBINARY:
			case Types.BLOB:
			case Types.ARRAY:
			case Types.SQLXML:
			value = checkForSimulatedTypesOrUseDefault(database, typeName, DBLargeBinary.class);
			break;
			default:
				throw new UnknownJavaSQLTypeException("Unknown Java SQL Type: " + columnType, columnType);
		}
		return value;
	}

	private static Class<? extends Object> checkForSimulatedTypesOrUseDefault(DBDatabase database, String typeName, final Class<?> defaultClass) throws NoAvailableDatabaseException {
		Class<? extends Object> value;
		Class<? extends QueryableDatatype<?>> customStringType = database.getDefinition().getQueryableDatatypeClassForSQLDatatype(typeName);
		if (customStringType != null) {
			value = customStringType;
			return value;
		} else {
			value = defaultClass;
			return value;
		}
	}

	/**
	 *
	 * returns a good guess at the java CLASS version of a DB field name.
	 *
	 * I.e. changes "_" into an uppercase letter.
	 *
	 * @param s	s
	 *
	 *
	 * @return camel case version of the String
	 */
	public static String toClassCase(String s) {
		StringBuilder classCaseString = new StringBuilder("");
		if (s == null) {
			return null;
		} else if (s.matches("[lLtT]+_[0-9]+(_[0-9]+)*")) {
			classCaseString.append(s.toUpperCase());
		} else {
			String[] parts = s.split("[^a-zA-Z0-9]");
			for (String part : parts) {
				classCaseString.append(toProperCase(part));
			}
		}
		return classCaseString.toString();
	}

	/**
	 *
	 * Capitalizes the first letter of the string
	 *
	 *
	 *
	 *
	 *
	 * @param s
	 * @return Capitalizes the first letter of the string
	 */
	public static String toProperCase(String s) {
		switch (s.length()) {
			case 0:
				return s;
			case 1:
				return s.toUpperCase();
			default:
				String firstChar = s.substring(0, 1);
				String rest;
				if (s.replaceAll("[^A-Z]", "").length() > 0
						&& s.replaceAll("[^a-z]", "").length() > 0) {
					rest = s.substring(1);//.toLowerCase();
				} else {
					rest = s.substring(1).toLowerCase();
				}
				if (firstChar.matches("[^a-zA-Z]")) {
					return "_" + firstChar + rest;
				} else {
					return firstChar.toUpperCase() + rest;
				}
		}
	}

	/**
	 *
	 * returns a good guess at the java field version of a DB field name.I.e.
	 *
	 * changes "_" into an uppercase letter.
	 *
	 *
	 *
	 *
	 *
	 * @param s
	 * @return Camel Case version of S
	 */
	public static String toFieldCase(String s) {
		String classClass = Utility.toClassCase(s);
		String camelCaseString = classClass.substring(0, 1).toLowerCase() + classClass.substring(1);
		camelCaseString = camelCaseString.replaceAll("[^a-zA-Z0-9_$]", "_");
		if (JAVA_RESERVED_WORDS.contains(camelCaseString)) {
			camelCaseString += "_";
		}
		return camelCaseString;
	}
}
