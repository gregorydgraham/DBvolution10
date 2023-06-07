/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.generation;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author gregorygraham
 */
public class FKBasedFKRecognisor extends ForeignKeyRecognisor {

	static final Pattern FK_START_PATTERN = Pattern.compile("^[fF][kK]_");

	public FKBasedFKRecognisor() {
	}

	/**
	 * Indicates that the column is a foreign key if the column name starts with
	 * "fk_".
	 *
	 * @param tableName tableName
	 * @param columnName columnName
	 * @return TRUE if the column is a foreign key column, FALSE otherwise
	 */
	@Override
	public boolean isForeignKeyColumn(String tableName, String columnName) {
		String toLowerCase = columnName.toLowerCase();
		boolean result = toLowerCase.startsWith("fk_");
		return result;
	}

	/**
	 * Converts the foreign key to the referenced column name.
	 *
	 * @param tableName tableName
	 * @param columnName columnName
	 * @return The name of the referenced column
	 */
	@Override
	public String getReferencedColumn(String tableName, String columnName) {
		String result;
		if (isForeignKeyColumn(tableName, columnName)) {
			Matcher matcher = FK_START_PATTERN.matcher(columnName);
			String firstReplace = matcher.replaceAll("uid_");
			result = firstReplace.replaceAll("^(uid_[a-zA-Z0-9]+)(_[0-9]*)*$", "$1");
		} else {
			result = null;
		}
		return result;
	}

	/**
	 * Converts the column name into the name of the referenced table.
	 *
	 * @param tableName tableName
	 * @param columnName columnName
	 * @return the name of the referenced table
	 */
	@Override
	public String getReferencedTable(String tableName, String columnName) {
		if (isForeignKeyColumn(tableName, columnName)) {
			Matcher matcher = FK_START_PATTERN.matcher(columnName);
			String strippedOfFK = matcher.replaceAll("");
			if (strippedOfFK.matches("^[0-9_]+$")) {
				String replaceAll = strippedOfFK.replaceAll("^([a-zA-Z0-9]+)(_[0-9]*)*$", "$1");
				return "T_" + replaceAll;
			} else {
				String replaceAll = strippedOfFK.replaceAll("_[0-9]+$", "");
				return Utility.toClassCase(replaceAll);
			}
		} else {
			return null;
		}
	}

}
