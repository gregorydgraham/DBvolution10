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

import com.informix.jdbc.IfxDriver;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author gregory.graham
 */
public class InformixDB extends DBDatabase {

    public final static String INFORMIXDRIVERNAME = "com.informix.jdbc.IfxDriver";
    private IfxDriver informixDriver;
    private SimpleDateFormat dateFormat;
    private String DATE_FORMAT = "yyyy-MM-dd hh:mm:ss";
    //TO_DATE("1998-07-07 10:24",   "%Y-%m-%d %H:%M")

    public InformixDB(String jdbcURL, String username, String password) throws ClassNotFoundException, SQLException {
        super(INFORMIXDRIVERNAME, jdbcURL, username, password);
        this.dateFormat = new SimpleDateFormat(DATE_FORMAT);
//        Class.forName("com.informix.jdbc.IfxDriver");
//        this.informixDriver = new IfxDriver();
//        try {
//            this.informixDriver.connect(jdbcURL, null);
//        } catch (SQLException exp) {
//            throw new RuntimeException("Unable to connect to URL: '" + "'. Please check your URL and database driver.", exp);
//        }
//        int majorVersion = informixDriver.getMajorVersion();
//        int minorVersion = informixDriver.getMinorVersion();
//        System.out.println("Using Informix " + majorVersion + "." + minorVersion);
    }

    @Override
    public String getDateFormattedForQuery(Date date) {
        return "TO_DATE('" + dateFormat.format(date) + "','%Y-%m-%d %H:%M:%S')";
    }
}
