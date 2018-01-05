/**
 * <h2>Core DBvolution Classes</h2>
 * <p>
 * DBvolution removes Object/Relational Impedance by using classes to represent
 * and store the information on each table, and using special datatypes to make
 * setting query conditions trivial and intuitive.
 * <p>
 * Demonstration application available at
 * <a href="https://github.com/gregorydgraham/DBvolution-Demo">gitHub</a> and
 * there are more docs and examples at
 * <a href="http://dbvolution.gregs.co.nz">dbvolution.gregs.co.nz</a>.
 *
 * <p>
 * To use DBV, first create a connection to your database using the DBDatabase
 * subclasses in {@link nz.co.gregs.dbvolution.databases}.
 * <p>
 * With the the DBDatabase you can create the DBRow classes required from the
 * database schema using
 * {@link nz.co.gregs.dbvolution.generation.DBTableClassGenerator} or you can
 * write your own. DBDatabase provides methods to create and "drop" table from
 * the database so you can start from scratch or migrate easily. There are
 * examples to start from in {@link nz.co.gregs.dbvolution.example}.
 * <p>
 * Using your {@link nz.co.gregs.dbvolution.DBRow DBRow subclasses} you can
 * create simple queries using
 * {@link nz.co.gregs.dbvolution.databases.DBDatabase#getDBTable(nz.co.gregs.dbvolution.DBRow) DBTable}
 * or complex ones using
 * {@link nz.co.gregs.dbvolution.databases.DBDatabase#getDBQuery(nz.co.gregs.dbvolution.DBRow...) DBQuery}.
 * <p>
 * You can create transactions easily using
 * {@link  nz.co.gregs.dbvolution.DBScript} testing the transaction with
 * {@link nz.co.gregs.dbvolution.DBScript#test(nz.co.gregs.dbvolution.databases.DBDatabase) test}
 * or committing the changes with
 * {@link nz.co.gregs.dbvolution.DBScript#implement(nz.co.gregs.dbvolution.databases.DBDatabase) implement}.
 * <p>
 * DBvolution automatically protects you from common SQL mistakes like Cartesian
 * Joins or Blank Queries, makes outer joins trivial, helps you collect semantic
 * knowledge, and makes your application completely portable across databases.
 * <p>
 * I've been writing SQL for 20 years and DBV is better, I hope you like it.
 * <p>
 * <b>Gregory Graham</b>
 */
package nz.co.gregs.dbvolution;
