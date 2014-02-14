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
package nz.co.gregs.dbvolution.datatypes;

import java.sql.Timestamp;
import java.util.Date;


public class DBDateOnly extends DBDate {
    
    public static final long serialVersionUID = 1L;

    public DBDateOnly() {
    }

    public DBDateOnly(Date date) {
        super(date);
    }

    public DBDateOnly(Timestamp timestamp) {
        super(timestamp);
    }

    @Override
    public String getSQLDatatype() {
        return "DATE";
    }
    
    @Override
    @SuppressWarnings("deprecation")
    public Date dateValue(){
        Date dateValue = super.dateValue();
        dateValue.setHours(0);
        dateValue.setMinutes(0);
        dateValue.setSeconds(0);
        return dateValue;
    }

}
