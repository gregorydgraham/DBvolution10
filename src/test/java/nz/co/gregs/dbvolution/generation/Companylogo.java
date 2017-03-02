package nz.co.gregs.dbvolution.generation;

import nz.co.gregs.dbvolution.*;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.annotations.*;

@DBTableName("COMPANYLOGO")
public class Companylogo extends DBRow {

	public static final long serialVersionUID = 1L;

	@DBColumn("LOGO_ID")
	@DBPrimaryKey
	public DBInteger logoId = new DBInteger();

	@DBColumn("CAR_COMPANY_FK")
	public DBInteger carCompanyFk = new DBInteger();

	@DBColumn("IMAGE_FILE")
	public DBLargeBinary imageFile = new DBLargeBinary();

	@DBColumn("IMAGE_NAME")
	public DBString imageName = new DBString();

}
