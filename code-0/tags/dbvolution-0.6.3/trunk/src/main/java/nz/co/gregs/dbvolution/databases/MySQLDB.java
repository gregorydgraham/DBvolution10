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

import java.util.Date;

/**
 *
 * @author gregory.graham
 */
public class MySQLDB extends DBDatabase{
    
    public final static String MYSQLDRIVERNAME = "com.mysql.jdbc.Driver";
    
    public MySQLDB(String jdbcURL, String username, String password){
        super(MYSQLDRIVERNAME, jdbcURL, username, password);
    }

    @Override
    public String getDateFormattedForQuery(Date date) {
        //SELECT STR_TO_DATE('01,5,2013','%d,%m,%Y');
        //SELECT STR_TO_DATE('09:30:17','%h:%i:%s');
        
        return " STR_TO_DATE('"
                +date.getDate()+","
                +(date.getMonth()+1)+","
                +(date.getYear()+1900)+" "
                +date.getHours()+":"
                +date.getMinutes()+":"
                +date.getSeconds()
                +"', '%d,%m,%Y %H:%i:%s') ";
        
    }
    
}
