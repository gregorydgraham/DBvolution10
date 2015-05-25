package nz.co.gregs.dbvolution.generation;

import nz.co.gregs.dbvolution.*;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.annotations.*;

@DBTableName("CAR_COMPANY")
public class CarCompany extends DBRow {

	public static final long serialVersionUID = 1L;

	@DBColumn("NAME")
	public DBString name = new DBString();

	@DBColumn("UID_CARCOMPANY")
	@DBPrimaryKey
	public DBInteger uidCarcompany = new DBInteger();

}
