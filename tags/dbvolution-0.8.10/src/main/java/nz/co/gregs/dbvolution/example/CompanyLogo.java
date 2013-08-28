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
package nz.co.gregs.dbvolution.example;

import nz.co.gregs.dbvolution.datatypes.DBByteArray;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;

/**
 *
 * @author gregorygraham
 */
public class CompanyLogo extends DBRow {

    public static final long serialVersionUID = 1L;
    
    @DBPrimaryKey
    @DBColumn("logo_id")
    public DBInteger logoID = new DBInteger();
    @DBForeignKey(CarCompany.class)
    @DBColumn("car_company_fk")
    public DBInteger carCompany = new DBInteger();
    @DBColumn("image_file")
    public DBByteArray imageBytes = new DBByteArray();
    @DBColumn("image_name")
    public DBString imageFilename = new DBString();
}
