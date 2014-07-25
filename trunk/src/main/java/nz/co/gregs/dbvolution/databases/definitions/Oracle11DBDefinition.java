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
package nz.co.gregs.dbvolution.databases.definitions;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.query.QueryOptions;
import nz.co.gregs.dbvolution.query.RowDefinition;

public class Oracle11DBDefinition extends DBDefinition {

    String dateFormatStr = "yyyy-M-d HH:mm:ss Z";
    String oracleDateFormatStr = "YYYY-MM-DD HH24:MI:SS TZHTZM";//*/"YYYY-M-DD HH24:mi:SS TZR";
    SimpleDateFormat javaToStringFormatter = new SimpleDateFormat(dateFormatStr);

    @Override
    public String getDateFormattedForQuery(Date date) {
        if (date == null) {
            return getNull();
        }
//        yyyy-MM-dd hh:mm:ss[.nnnnnnnnn]
        return " TO_TIMESTAMP_TZ('" + javaToStringFormatter.format(date) + "','" + oracleDateFormatStr + "') ";
        //return "'"+strToDateFormat.format(date)+"'";
    }

    @Override
    public String formatTableName(DBRow table) {
        return table.getTableName();
    }

    @Override
    public String formatColumnName(String columnName) {
        return "" + columnName + "";
    }

//	@Override
//	public String formatForColumnAlias(final String actualName) {
//		String formattedName = actualName.replaceAll("\\.", "__");
//		return ("DB" + formattedName.hashCode()).replaceAll("-", "O");
//	}

	@Override
	public Object getTableAlias(RowDefinition tabRow) {
		return ("O" + tabRow.getClass().getSimpleName().hashCode()).replaceAll("-", "O");
	}

	@Override
	public String beginTableAlias() {
		return " ";
	}
	
	@Override
    public String getSQLTypeOfDBDatatype(QueryableDatatype qdt) {
        if (qdt instanceof DBBoolean) {
            return " NUMBER(1)";
        } else if (qdt instanceof DBString) {
            return " VARCHAR2(1000) ";
        } else if (qdt instanceof DBDate) {
            return " TIMESTAMP ";
//        } else if (qdt instanceof DBLargeObject) {
//            return " LONGBLOB ";
        } else {
            return qdt.getSQLDatatype();
        }
    }

//    @Override
//    public boolean prefersIndexBasedGroupByClause() {
//        return true;
//    }

    @Override
    public Object endSQLStatement() {
        return "";
    }

    @Override
    public String endInsertLine() {
        return "";
    }

    @Override
    public String endDeleteLine() {
        return "";
    }

    @Override
    public Object getLimitRowsSubClauseDuringSelectClause(QueryOptions options) {
        return "/*+ FIRST_ROWS(" + options.getRowLimit() + ") */";
    }

	@Override
	public Object getLimitRowsSubClauseAfterWhereClause(QueryOptions options) {
		return "";
	}

//    @Override
//    public Object getLimitRowsSubClauseAfterWhereClause(Long rowLimit) {
//        return "";
//    }

//    @Override
//    public String getCurrentDateFunctionName() {
//        return "SYSDATE";
//    }
//
//    @Override
//    public String getCurrentTimestampFunction() {
//        return "SYSDATE";
//    }
//
//    @Override
//    public String getCurrentTimeFunction() {
//        return "SYSDATE";
//    }

    @Override
    public String getCurrentUserFunctionName() {
        return "USER";
    }

    @Override
    public String getPositionFunction(String originalString, String stringToFind) {
        return "INSTR(" + originalString + "," + stringToFind + ")";
    }

    @Override
    public String getIfNullFunctionName() {
        return "ISNULL"; //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean supportsPaging(QueryOptions options) {
        return false;
    }

	@Override
	public String getColumnAutoIncrementSuffix() {
		return "";
	}

	@Override
	public boolean usesTriggerBasedIdentities() {
		return true;
	}
	
	@Override
	public List<String> getTriggerBasedIdentitySQL(String table, String column){
//		    CREATE SEQUENCE dept_seq;
//
//Create a trigger to populate the ID column if it's not specified in the insert.
//
//    CREATE OR REPLACE TRIGGER dept_bir 
//    BEFORE INSERT ON departments 
//    FOR EACH ROW
//    WHEN (new.id IS NULL)
//    BEGIN
//      SELECT dept_seq.NEXTVAL
//      INTO   :new.id
//      FROM   dual;
//    END;
		
		List<String> result = new ArrayList<String>();
		String sequenceName = table+"_"+column+"_dbv_seq";
		result.add("CREATE SEQUENCE "+sequenceName);
		
		result.add("CREATE OR REPLACE TRIGGER "+table+"_"+column+"_dbv_trg \n" +
"    BEFORE INSERT ON "+table+" \n" +
"    FOR EACH ROW\n" +
"    WHEN (new."+column+" IS NULL)\n" +
"    BEGIN\n" +
"      SELECT "+sequenceName+".NEXTVAL\n" +
"      INTO   :new."+column+"\n" +
"      FROM   dual;\n" +
"    END");
		
		return result;
	}
	
	@Override
	public String getStringLengthFunctionName() {
		return "LENGTH";
	}

	@Override
	public String doSubstringTransform(String originalString, String start, String length) {
		return " SUBSTR("
				+ originalString
				+ ", "
				+ start
				+ (length.trim().isEmpty() ? "" : ", " + length)
				+ ") ";
	}

	@Override
	public boolean supportsRadiansFunction() {
		return false;
	}

	@Override
	public boolean supportsDegreesFunction() {
		return false;
	}
}
