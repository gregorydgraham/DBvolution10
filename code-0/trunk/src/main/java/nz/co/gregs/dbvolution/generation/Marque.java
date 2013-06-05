package nz.co.gregs.dbvolution.generation;
import nz.co.gregs.dbvolution.*;
import nz.co.gregs.dbvolution.annotations.*;
@DBTableName("MARQUE") 
public class Marque extends DBTableRow {
    @DBTableColumn("NUMERIC_CODE")
    public DBNumber numericCode = new DBNumber();
    @DBTableColumn("UID_MARQUE")
    public DBInteger uidMarque = new DBInteger();
    @DBTableColumn("ISUSEDFORTAFROS")
    public DBString isusedfortafros = new DBString();
    @DBTableColumn("FK_TOYSTATUSCLASS")
    public DBNumber fkToystatusclass = new DBNumber();
    @DBTableColumn("INTINDALLOCALLOWED")
    public DBString intindallocallowed = new DBString();
    @DBTableColumn("UPD_COUNT")
    public DBInteger updCount = new DBInteger();
    @DBTableColumn("AUTO_CREATED")
    public DBString autoCreated = new DBString();
    @DBTableColumn("NAME")
    public DBString name = new DBString();
    @DBTableColumn("PRICINGCODEPREFIX")
    public DBString pricingcodeprefix = new DBString();
    @DBTableColumn("RESERVATIONSALWD")
    public DBString reservationsalwd = new DBString();
    @DBTableColumn("CREATION_DATE")
    public DBDate creationDate = new DBDate();
}