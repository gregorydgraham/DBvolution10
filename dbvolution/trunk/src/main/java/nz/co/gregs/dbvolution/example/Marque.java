package nz.co.gregs.dbvolution.example;

import nz.co.gregs.dbvolution.DBDate;
import nz.co.gregs.dbvolution.DBNumber;
import nz.co.gregs.dbvolution.DBString;
import nz.co.gregs.dbvolution.DBTableRow;
import nz.co.gregs.dbvolution.annotations.DBTableColumn;
import nz.co.gregs.dbvolution.annotations.DBTableForeignKey;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.annotations.DBTablePrimaryKey;

/**
 *
 * @author gregory.graham
 */
@DBTableName("marque")
public class Marque extends DBTableRow {

    /*
     * create view "mdamgr".marque
     * (numeric_code,uidMarque,isusedfortafros,fk_toystatusclass,intindallocallowed,upd_count,auto_created,name,pricingcodeprefix,reservationsalwd)
     * as
     */
    @DBTableColumn("numeric_code")
    private DBNumber numericCode = new DBNumber();
    @DBTableColumn("uid_marque")
    @DBTablePrimaryKey
    private DBNumber uidMarque = new DBNumber();
    @DBTableColumn("isusedfortafros")
    private DBString isUsedForTAFROs = new DBString();
    @DBTableColumn("fk_toystatusclass")
    @DBTableForeignKey("\"mdamgr\".toystatusclass")
    private DBNumber toyotaStatusClassID = new DBNumber();
    @DBTableColumn("intindallocallowed")
    private DBString intIndividualAllocationsAllowed = new DBString();
    @DBTableColumn("upd_count")
    private DBNumber updateCount = new DBNumber();
    @DBTableColumn
    private DBString auto_created = new DBString();
    @DBTableColumn
    private DBString name = new DBString();
    @DBTableColumn("pricingcodeprefix")
    private DBString pricingCodePrefix = new DBString();
    @DBTableColumn("reservationsalwd")
    private DBString reservationsAllowed = new DBString();
    @DBTableColumn("creation_date")
    private DBDate creationDate = new DBDate();

    /**
     * @return the numericCode
     */
    public DBNumber getNumericCode() {
        return numericCode;
    }

    /**
     * @param numericCode the numericCode to set
     */
    public void setNumericCode(DBNumber numericCode) {
        this.numericCode = numericCode;
    }

    /**
     * @return the uidMarque
     */
    public DBNumber getUidMarque() {
        return uidMarque;
    }

    /**
     * @param uidMarque the uidMarque to set
     */
    public void setUidMarque(DBNumber uidMarque) {
        this.uidMarque = uidMarque;
    }

    /**
     * @return the isUsedForTAFROs
     */
    public DBString getIsUsedForTAFROs() {
        return isUsedForTAFROs;
    }

    /**
     * @param isUsedForTAFROs the isUsedForTAFROs to set
     */
    public void setIsUsedForTAFROs(DBString isUsedForTAFROs) {
        this.isUsedForTAFROs = isUsedForTAFROs;
    }

    /**
     * @return the toyotaStatusClassID
     */
    public DBNumber getToyotaStatusClassID() {
        return toyotaStatusClassID;
    }

    /**
     * @param toyotaStatusClassID the toyotaStatusClassID to set
     */
    public void setToyotaStatusClassID(DBNumber toyotaStatusClassID) {
        this.toyotaStatusClassID = toyotaStatusClassID;
    }

    /**
     * @return the intIndividualAllocationsAllowed
     */
    public DBString getIntIndividualAllocationsAllowed() {
        return intIndividualAllocationsAllowed;
    }

    /**
     * @param intIndividualAllocationsAllowed the
     * intIndividualAllocationsAllowed to set
     */
    public void setIntIndividualAllocationsAllowed(DBString intIndividualAllocationsAllowed) {
        this.intIndividualAllocationsAllowed = intIndividualAllocationsAllowed;
    }

    /**
     * @return the updateCount
     */
    public DBNumber getUpdateCount() {
        return updateCount;
    }

    /**
     * @param updateCount the updateCount to set
     */
    public void setUpdateCount(DBNumber updateCount) {
        this.updateCount = updateCount;
    }

    /**
     * @return the auto_created
     */
    public DBString getAuto_created() {
        return auto_created;
    }

    /**
     * @param auto_created the auto_created to set
     */
    public void setAuto_created(DBString auto_created) {
        this.auto_created = auto_created;
    }

    /**
     * @return the name
     */
    public DBString getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(DBString name) {
        this.name = name;
    }

    /**
     * @return the pricingCodePrefix
     */
    public DBString getPricingCodePrefix() {
        return pricingCodePrefix;
    }

    /**
     * @param pricingCodePrefix the pricingCodePrefix to set
     */
    public void setPricingCodePrefix(DBString pricingCodePrefix) {
        this.pricingCodePrefix = pricingCodePrefix;
    }

    /**
     * @return the reservationsAllowed
     */
    public DBString getReservationsAllowed() {
        return reservationsAllowed;
    }

    /**
     * @param reservationsAllowed the reservationsAllowed to set
     */
    public void setReservationsAllowed(DBString reservationsAllowed) {
        this.reservationsAllowed = reservationsAllowed;
    }

    /**
     * @return the creationDate
     */
    public DBDate getCreationDate() {
        return creationDate;
    }

    /**
     * @param creationDate the creationDate to set
     */
    public void setCreationDate(DBDate creationDate) {
        this.creationDate = creationDate;
    }
}
