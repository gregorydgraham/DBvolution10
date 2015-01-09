/**
 * DBDefinition Subclasses For Supported Databases.
 * <p>
 * DBDefinitions hold all the SQL snippets required to make correct SQL scripts.
 * <p>
 * Each subclass of DBDefinition contains the changes to the standard definition
 * required for the particular database.
 * <p>
 * Hopefully you don't need to use a DBDDefinition (it should be included for
 * free in the corresponding DBDatabase) but if you are implementing support for
 * a new database this is where you will do most of the changes.
 * <p>
 * I've been writing SQL for 20 years and DBV is easier, I hope you like it.
 * <p>
 * <b>Gregory Graham</b>
 */
package nz.co.gregs.dbvolution.databases.definitions;
