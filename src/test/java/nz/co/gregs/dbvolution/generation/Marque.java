package nz.co.gregs.dbvolution.generation;

import nz.co.gregs.dbvolution.*;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.annotations.*;

@DBTableName("MARQUE")
public class Marque extends DBRow {

	public static final long serialVersionUID = 1L;

	@DBColumn("NUMERIC_CODE")
	public DBNumber numericCode = new DBNumber();

	@DBColumn("UID_MARQUE")
	@DBPrimaryKey
	public DBInteger uidMarque = new DBInteger();

	@DBColumn("ISUSEDFORTAFROS")
	public DBString isusedfortafros = new DBString();

	@DBColumn("FK_TOYSTATUSCLASS")
	public DBNumber fkToystatusclass = new DBNumber();

	@DBColumn("INTINDALLOCALLOWED")
	public DBString intindallocallowed = new DBString();

	@DBColumn("UPD_COUNT")
	public DBInteger updCount = new DBInteger();

	@DBColumn("AUTO_CREATED")
	public DBString autoCreated = new DBString();

	@DBColumn("NAME")
	public DBString name = new DBString();

	@DBColumn("PRICINGCODEPREFIX")
	public DBString pricingcodeprefix = new DBString();

	@DBColumn("RESERVATIONSALWD")
	public DBString reservationsalwd = new DBString();

	@DBColumn("CREATION_DATE")
	public DBDate creationDate = new DBDate();

	@DBColumn("ENABLED")
	public DBBoolean enabled = new DBBoolean();

	@DBColumn("FK_CARCOMPANY")
	public DBInteger fkCarcompany = new DBInteger();

}
