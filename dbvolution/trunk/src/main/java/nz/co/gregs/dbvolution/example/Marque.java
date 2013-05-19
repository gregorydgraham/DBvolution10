package nz.co.gregs.dbvolution.example;

import nz.co.gregs.dbvolution.*;
import nz.co.gregs.dbvolution.annotations.*;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.MySQLDB;
import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.Date;

/**
 *
 * @author gregory.graham
 */
@DBTableName("superconductor.marque")
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
     *
     * @param args
     * @throws SQLException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws NoSuchMethodException
     * @throws IllegalArgumentException
     * @throws InvocationTargetException
     * @throws ClassNotFoundException
     * @throws IntrospectionException
     */
    public static void main(String[] args) throws SQLException, InstantiationException, IllegalAccessException, NoSuchMethodException, IllegalArgumentException, InvocationTargetException, ClassNotFoundException, IntrospectionException {

        String driverName = "com.mysql.jdbc.Driver";
        String jdbcURL = "jdbc:mysql://localhost:3306/superconductor?zeroDateTimeBehavior=convertToNull";
        String username = "root";
        String password = null;

        DBDatabase myDatabase = new MySQLDB(jdbcURL, username, password);

        DBTable.setPrintSQLBeforeExecuting(true);
        DBTable<Marque> marques = new DBTable<Marque>(new Marque(), myDatabase);
        marques.getAllRows();
        for (Marque row : marques) {
            System.out.println(row);
        }

        Marque marque = marques.firstRow();
        if (marque != null) {
            String primaryKey = marque.getPrimaryKey();
            DBTable<Marque> singleMarque = new DBTable<Marque>(new Marque(), myDatabase);
            singleMarque.getByPrimaryKey(primaryKey).printAllRows();


            DBTable<Marque> byPrimaryKey = marques.getByPrimaryKey(1);
            System.out.println(byPrimaryKey.firstRow());
        }
        Marque marqueQuery = new Marque();
        marqueQuery.getName().isLike("%T%");
        marqueQuery.getNumericCode().isBetween(0, 90000000);
        //System.out.println(marques.getSQLForExample(marqueQuery));
        marques = marques.getByExample(marqueQuery);
        for (Marque row : marques) {
            System.out.println(row);
        }

        Marque hummerQuery = new Marque();
        hummerQuery.getUidMarque().isLiterally(1L);
        marques = marques.getByExample(hummerQuery);
        marques.printAllRows();

        hummerQuery.getUidMarque().blankQuery();
        hummerQuery.getName().isIn(new String[]{"TOYOTA","HUMMER"});
        marques = marques.getByExample(hummerQuery);
        marques.printAllRows();

        Marque oldQuery = new Marque();
        oldQuery.creationDate.isBetween(new Date(0L), new Date());
        marques = marques.getByExample(oldQuery);
        marques.printAllRows();

        String rawQuery = "and lower(name) in ('toyota','hummer') ;  ";
        marques = marques.getByRawSQL(rawQuery);
        marques.printAllRows();
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
