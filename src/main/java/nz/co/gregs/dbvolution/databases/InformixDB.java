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
package nz.co.gregs.dbvolution.databases;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author gregory.graham
 */
public class InformixDB extends DBDatabase {

    public final static String INFORMIXDRIVERNAME = "com.informix.jdbc.IfxDriver";
    private SimpleDateFormat dateFormat;
    private String DATE_FORMAT = "yyyy-MM-dd hh:mm:ss";
    //TO_DATE("1998-07-07 10:24",   "%Y-%m-%d %H:%M")

    public InformixDB(String jdbcURL, String username, String password) throws ClassNotFoundException, SQLException {
        super(INFORMIXDRIVERNAME, jdbcURL, username, password);
        this.dateFormat = new SimpleDateFormat(DATE_FORMAT);
    }

    @Override
    public String getDateFormattedForQuery(Date date) {
        return "TO_DATE('" + dateFormat.format(date) + "','%Y-%m-%d %H:%M:%S')";
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
    public Object getTopClause(Long rowLimit) {
        return " FIRST "+rowLimit+" ";
    }

}
