/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.List;
import nz.co.gregs.dbvolution.query.RowDefinition;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public abstract class OutputFormat {

	public static final TabSeparated TSV = new TabSeparated();
	public static final CSV CSV = new CSV();
	public static final HTMLTable HTMLTABLE = new HTMLTable();

	public OutputFormat() {
		super();
	}

	public String formatDBQueryRows(SimpleDateFormat DATETIME_FORMAT, List<DBQueryRow> rows) {
		return formatDBQueryRows("", "", "", DATETIME_FORMAT, rows);
	}

	public String formatDBRows(SimpleDateFormat DATETIME_FORMAT, RowDefinition... rows) {
		return OutputFormat.this.formatDBRows("", "", "", DATETIME_FORMAT, rows);
	}

	public String formatDBRows(String headerRowStyle, String headerCellStyle, String rowStyle, SimpleDateFormat dateFormat, RowDefinition... rows) {
		StringBuilder result = new StringBuilder(formatHeader(rows[0], headerRowStyle, headerCellStyle));
		for (RowDefinition row : rows) {
			result.append(formatRow(row, rowStyle, dateFormat));
		}
		return result.toString();
	}

	public String formatDBQueryRows(String headerRowStyle, String headerCellStyle, String rowStyle, SimpleDateFormat dateFormat, List<DBQueryRow> queryRows) {
		StringBuilder result = new StringBuilder(formatHeader(queryRows.get(0), headerRowStyle, headerCellStyle));
		for (DBQueryRow row : queryRows) {
			result.append(formatRow(row, rowStyle, dateFormat));
		}
		return result.toString();
	}

	String formatRow(RowDefinition row, String tableRowCSSClass, SimpleDateFormat dateFormat) {
		int fieldCount = 0;
		StringBuilder string = new StringBuilder();
		Collection<String> fieldValues = row.getFieldValues(dateFormat);
//		List<PropertyWrapper> fields = row.getWrapper().getColumnPropertyWrappers();

		string.append(getRowStart(tableRowCSSClass));
		for (String value : fieldValues) {
			if (fieldCount > 0) {
				string.append(getRowFieldBetween());
			}
			string.append(getRowFieldPrefix(tableRowCSSClass));
			string.append(value);
			final String rowFieldSuffix = getRowFieldSuffix(tableRowCSSClass);
			string.append(rowFieldSuffix);
			fieldCount++;
		}
		string.append(getRowEnd(tableRowCSSClass));
		return string.toString();
	}

	String formatHeader(RowDefinition row, String headerRowCSSClass, String headerCellCSSClass) {
		int fieldCount = 0;
		StringBuilder string = new StringBuilder();
		List<String> fields = row.getFieldNames();

		string.append(getHeaderStart(headerRowCSSClass));
		for (String field : fields) {
			if (fieldCount > 0) {
				string.append(getHeaderFieldBetween());
			}
			string.append(getHeaderFieldPrefix(headerCellCSSClass));
			string.append(field);
			string.append(getHeaderFieldSuffix(headerRowCSSClass));
			fieldCount++;
		}
		string.append(getHeaderEnd(headerRowCSSClass));
		return string.toString();
	}

	String formatRow(DBQueryRow row, String tableRowCSSClass, SimpleDateFormat dateFormat) {
		int fieldCount = 0;
		StringBuilder string = new StringBuilder();
		Collection<String> fieldValues = row.getFieldValues(dateFormat);
//		List<PropertyWrapper> fields = row.getWrapper().getColumnPropertyWrappers();

		string.append(getRowStart(tableRowCSSClass));
		for (String value : fieldValues) {
			if (fieldCount > 0) {
				string.append(getRowFieldBetween());
			}
			string.append(getRowFieldPrefix(tableRowCSSClass));
			string.append(value);
			final String rowFieldSuffix = getRowFieldSuffix(tableRowCSSClass);
			string.append(rowFieldSuffix);
			fieldCount++;
		}
		string.append(getRowEnd(tableRowCSSClass));
		return string.toString();
	}

	String formatHeader(DBQueryRow row, String headerRowCSSClass, String headerCellCSSClass) {
		int fieldCount = 0;
		StringBuilder string = new StringBuilder();
		List<String> fields = row.getFieldNames();

		string.append(getHeaderStart(headerRowCSSClass));
		for (String field : fields) {
			if (fieldCount > 0) {
				string.append(getHeaderFieldBetween());
			}
			string.append(getHeaderFieldPrefix(headerCellCSSClass));
			string.append(field);
			string.append(getHeaderFieldSuffix(headerRowCSSClass));
			fieldCount++;
		}
		string.append(getHeaderEnd(headerRowCSSClass));
		return string.toString();
	}

	abstract String getRowStart(String tableRowCSSClass);

	abstract String getHeaderStart(String tableRowCSSClass);

	abstract String getHeaderEnd(String tableRowCSSClass);

	abstract String getHeaderFieldPrefix(String tableCellCSSClass);

	abstract String getHeaderFieldSuffix(String tableRowCSSClass);

	abstract String getHeaderFieldBetween();

	abstract String getRowFieldPrefix(String tableRowCSSClass);

	abstract String getRowFieldSuffix(String tableRowCSSClass);

	abstract String getRowFieldBetween();

	abstract String getRowEnd(String tableRowCSSClass);

	protected static class TabSeparated extends OutputFormat {

		private TabSeparated() {
		}

		@Override
		String getRowStart(String tableRowCSSClass) {
			return "";
		}

		@Override
		String getHeaderStart(String tableRowCSSClass) {
			return "";
		}

		@Override
		String getHeaderEnd(String tableRowCSSClass) {
			return System.getProperty("line.separator");
		}

		@Override
		String getHeaderFieldSuffix(String tableRowCSSClass) {
			return "";
		}

		@Override
		String getHeaderFieldBetween() {
			return "\t";
		}

		@Override
		String getRowFieldSuffix(String tableRowCSSClass) {
			return "";
		}

		@Override
		String getRowFieldBetween() {
			return "\t";
		}

		@Override
		String getRowEnd(String tableRowCSSClass) {
			return System.getProperty("line.separator");
		}

		@Override
		String getHeaderFieldPrefix(String tableCellCSSClass) {
			return "";
		}

		@Override
		String getRowFieldPrefix(String tableRowCSSClass) {
			return "";
		}

	}

	public static class CSV extends OutputFormat {

		private CSV() {
		}

		@Override
		String getRowStart(String tableRowCSSClass) {
			return "";
		}

		@Override
		String getHeaderStart(String tableRowCSSClass) {
			return "";
		}

		@Override
		String getHeaderEnd(String tableRowCSSClass) {
			return System.getProperty("line.separator");
		}

		@Override
		String getHeaderFieldSuffix(String tableRowCSSClass) {
			return "\"";
		}

		@Override
		String getHeaderFieldBetween() {
			return ", ";
		}

		@Override
		String getRowFieldSuffix(String tableRowCSSClass) {
			return "\"";
		}

		@Override
		String getRowFieldBetween() {
			return ", ";
		}

		@Override
		String getRowEnd(String tableRowCSSClass) {
			return System.getProperty("line.separator");
		}

		@Override
		String getHeaderFieldPrefix(String tableCellCSSClass) {
			return "\"";
		}

		@Override
		String getRowFieldPrefix(String tableRowCSSClass) {
			return "\"";
		}
	}

	public static class HTMLTable extends OutputFormat {

		public HTMLTable() {
		}

		@Override
		protected String getRowStart(String tableRowCSSClass) {
			return "<tr class=\"" + tableRowCSSClass + "\">";
		}

		@Override
		protected String getHeaderStart(String tableRowCSSClass) {
			return "<tr class=\"" + tableRowCSSClass + "\">";
		}

		@Override
		protected String getHeaderEnd(String tableRowCSSClass) {
			return "</tr>" + System.getProperty("line.separator");
		}

		@Override
		protected String getHeaderFieldPrefix(String tableCellCSSClass) {
			return "<th class=\"" + tableCellCSSClass + "\">";
		}

		@Override
		protected String getHeaderFieldSuffix(String tableRowCSSClass) {
			return "</th>";
		}

		@Override
		protected String getHeaderFieldBetween() {
			return "";
		}

		@Override
		protected String getRowFieldPrefix(String tableFieldCSSClass) {
			return "<td class=\"" + tableFieldCSSClass + "\">";
		}

		@Override
		protected String getRowFieldSuffix(String tableRowCSSClass) {
			return "</td>";
		}

		@Override
		protected String getRowFieldBetween() {
			return "";
		}

		@Override
		protected String getRowEnd(String tableRowCSSClass) {
			return "</tr>" + System.getProperty("line.separator");
		}
	}

}
