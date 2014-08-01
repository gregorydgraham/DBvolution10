/**
 * DBvolution: Always Java, Never SQL.
 * <p>
 * DBvolution removes Object/Relational Impedance by using classes to represent
 * and store the information on each table, and using special datatypes to make
 * setting query conditions trivial and intuitive.
 * <p>
 * To use DBV, first create a connection to your database using the DBDatabase
 * subclasses in {@link nz.co.gregs.dbvolution.databases}.
 * <p>
 * With the the DBDatabase you can create to DBRow classes required from the
 * database schema using
 * {@link nz.co.gregs.dbvolution.generation.DBTableClassGenerator} or you can
 * write you own. There are examples to start from in
 * {@link nz.co.gregs.dbvolution.example}.
 * <p>
 * Using your {@link DBRow DBRow subclasses} you can create simple queries using
 * {@link DBDatabase#getDBTable(nz.co.gregs.dbvolution.DBRow) DBTable} or
 * complex ones using
 * {@link DBDatabase#getDBQuery(nz.co.gregs.dbvolution.DBRow...) DBQuery}.
 * <p>
 * You can create transactions easily using {@link DBScript} testing the
 * transaction with
 * {@link DBScript#test(nz.co.gregs.dbvolution.DBDatabase) test} or committing
 * the changes with
 * {@link DBScript#implement(nz.co.gregs.dbvolution.DBDatabase) implement}.
 * <p>
 * DBvolution automatically protects you from common SQL mistakes like Cartesian
 * Joins or Blank Queries, makes outer joins trivial, helps you collect semantic
 * knowledge, and makes your application completely portable across databases.
 * <p>
 * I've been writing SQL for 20 years and DBV is easier, I hope you like it.
 * <p>
 * <b>Gregory Graham<b>
 */
package nz.co.gregs.dbvolution.actions;
