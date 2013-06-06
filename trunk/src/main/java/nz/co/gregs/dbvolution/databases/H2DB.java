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

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author greg
 */
public class H2DB extends DBDatabase{
    
    SimpleDateFormat strToDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    
    public H2DB(String jdbcURL, String username, String password){
        super("org.h2.Driver",jdbcURL, username, password);
    }

    @Override
    public String getDateFormattedForQuery(Date date) {
        
//        yyyy-MM-dd hh:mm:ss[.nnnnnnnnn]
        
        return "'"+strToDateFormat.format(date)+"'";
    }
    
    @Override
    public String formatColumnName(String columnName){
        return columnName.toUpperCase();
    }
    
    /**
     * 
     * overrides standard method and uppercases everything
     *
     * @param tableName
     * @param columnName
     * @return
     */
    @Override
    public String formatTableAndColumnForDBTableForeignKey(String tableName, String columnName) {
        return super.formatTableAndColumnForDBTableForeignKey(tableName, columnName).toUpperCase();
    }
}
