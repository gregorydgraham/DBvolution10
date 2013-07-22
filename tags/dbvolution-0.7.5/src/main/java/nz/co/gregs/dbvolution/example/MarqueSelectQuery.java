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

import nz.co.gregs.dbvolution.DBInteger;
import nz.co.gregs.dbvolution.DBString;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.*;

/**
 *
 * @author gregorygraham
 */
@DBSelectQuery("select uid_marque, isusedfortafros from marque")
@DBTableName("marque")
public class MarqueSelectQuery extends DBRow {

    @DBColumn("uid_marque")
    @DBPrimaryKey
    public DBInteger uidMarque = new DBInteger();
    @DBColumn("isusedfortafros")
    public DBString isUsedForTAFROs = new DBString();
}
