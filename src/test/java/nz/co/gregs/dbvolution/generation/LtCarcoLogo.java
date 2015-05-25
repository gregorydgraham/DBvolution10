package nz.co.gregs.dbvolution.generation;

import nz.co.gregs.dbvolution.*;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.annotations.*;

@DBTableName("LT_CARCO_LOGO")
public class LtCarcoLogo extends DBRow {

	public static final long serialVersionUID = 1L;

	@DBColumn("FK_CAR_COMPANY")
	public DBInteger fkCarCompany = new DBInteger();

	@DBColumn("FK_COMPANY_LOGO")
	public DBInteger fkCompanyLogo = new DBInteger();

}
