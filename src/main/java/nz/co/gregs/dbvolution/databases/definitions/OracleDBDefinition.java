/*
 * Copyright 2013 gregory.graham.
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
import java.util.Date;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.*;


public class OracleDBDefinition extends DBDefinition {

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
        return "\""+columnName+"\"";
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
    public Object getLimitRowsSubClauseDuringSelectClause(Long rowLimit) {
        return "/*+ FIRST_ROWS("+rowLimit+") */"; 
    }

    @Override
    public Object getLimitRowsSubClauseAfterWhereClause(Long rowLimit) {
        return "";
    }

    @Override
    public String getCurrentDateFunctionName() {
        return "SYSDATE"; 
    }

    @Override
    public String getCurrentTimestampFunction() {
        return "SYSDATE"; 
    }

    @Override
    public String getCurrentTimeFunction() {
        return "SYSDATE"; 
    }

    @Override
    public String getCurrentUserFunctionName() {
        return "USER"; 
    }    
    
    @Override
    public String getPositionFunction(String originalString, String stringToFind) {
        return "INSTR("+originalString+","+stringToFind+")";
    }
}
