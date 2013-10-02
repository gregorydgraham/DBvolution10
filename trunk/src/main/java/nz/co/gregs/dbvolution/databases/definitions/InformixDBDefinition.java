/*
 * Copyright 2013 gregorygraham.
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


public class InformixDBDefinition extends DBDefinition {
    
    private SimpleDateFormat dateFormat;
    private String DATE_FORMAT = "yyyy-MM-dd hh:mm:ss";
    //TO_DATE("1998-07-07 10:24",   "%Y-%m-%d %H:%M")
    public String informixDateFormat = "%Y-%m-%d %H:%M:%S";

    public InformixDBDefinition(){
        
        this.dateFormat = new SimpleDateFormat(DATE_FORMAT);
    }

    @Override
    public String getDateFormattedForQuery(Date date) {
        return "TO_DATE('" + dateFormat.format(date) + "','"+informixDateFormat+"')";
    }

    /**
     *
     * @param tableName
     * @param columnName
     * @return
     */
    @Override
    public String formatTableAndColumnName(String tableName, String columnName) {
        return "" + tableName + "." + columnName + "";
    }

    @Override
    public Object getLimitRowsSubClauseDuringSelectClause(Long rowLimit) {
        return " FIRST "+rowLimit+" ";
    }    

    @Override
    public Object getLimitRowsSubClauseAfterWhereClause(Long rowLimit) {
        return "";
    }
    
    
}
