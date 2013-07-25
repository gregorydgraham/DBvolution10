package nz.co.gregs.dbvolution.example;

import java.util.Date;
import nz.co.gregs.dbvolution.*;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.annotations.DBPrimaryKey;

/**
 *
 * @author gregory.graham
 */
@DBTableName("marque")
public class Marque extends DBRow {

    /*
     * create view "mdamgr".marque
     * (numeric_code,uidMarque,isusedfortafros,fk_toystatusclass,intindallocallowed,upd_count,auto_created,name,pricingcodeprefix,reservationsalwd)
     * as
     */
    @DBColumn("numeric_code")
    public DBNumber numericCode = new DBNumber();
    @DBColumn("uid_marque")
    @DBPrimaryKey
    public DBInteger uidMarque = new DBInteger();
    @DBColumn("isusedfortafros")
    public DBString isUsedForTAFROs = new DBString();
    @DBColumn("fk_toystatusclass")
    public DBNumber toyotaStatusClassID = new DBNumber();
    @DBColumn("intindallocallowed")
    public DBString individualAllocationsAllowed = new DBString();
    @DBColumn("upd_count")
    public DBInteger updateCount = new DBInteger();
    @DBColumn
    public DBString auto_created = new DBString();
    @DBColumn
    public DBString name = new DBString();
    @DBColumn("pricingcodeprefix")
    public DBString pricingCodePrefix = new DBString();
    @DBColumn("reservationsalwd")
    public DBString reservationsAllowed = new DBString();
    @DBColumn("creation_date")
    public DBDate creationDate = new DBDate();
    
    @DBForeignKey(CarCompany.class)
    @DBColumn("fk_carcompany")
    public DBInteger carCompany = new DBInteger();


    public Marque(){
    }
    
    public Marque(int uidMarque, String isUsedForTAFROs, int statusClass, String intIndividualAllocationsAllowed, int updateCount, String autoCreated, String name, String pricingCodePrefix, String reservationsAllowed, Date creationDate, int carCompany) {
        this.uidMarque.isLiterally(uidMarque);
        this.isUsedForTAFROs.isLiterally(isUsedForTAFROs);
        toyotaStatusClassID.isLiterally(statusClass);
        this.individualAllocationsAllowed.isLiterally(intIndividualAllocationsAllowed);
        this.updateCount.isLiterally(updateCount);
        this.auto_created.isLiterally(autoCreated);
        this.name.isLiterally(name);
        this.pricingCodePrefix.isLiterally(pricingCodePrefix);
        this.reservationsAllowed.isLiterally(reservationsAllowed);
        this.creationDate.isLiterally(creationDate);
        this.carCompany.isLiterally(carCompany);
    }

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
    public DBInteger getUidMarque() {
        return uidMarque;
    }

    /**
     * @param uidMarque the uidMarque to set
     */
    public void setUidMarque(DBInteger uidMarque) {
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
        return individualAllocationsAllowed;
    }

    /**
     * @param intIndividualAllocationsAllowed the
     * intIndividualAllocationsAllowed to set
     */
    public void setIntIndividualAllocationsAllowed(DBString intIndividualAllocationsAllowed) {
        this.individualAllocationsAllowed = intIndividualAllocationsAllowed;
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
    public void setUpdateCount(DBInteger updateCount) {
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

    /**
     * @return the carCompany
     */
    public DBInteger getCarCompany() {
        return carCompany;
    }
}
