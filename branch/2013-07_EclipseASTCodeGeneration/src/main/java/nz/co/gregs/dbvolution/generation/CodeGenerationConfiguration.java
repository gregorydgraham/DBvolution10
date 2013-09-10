package nz.co.gregs.dbvolution.generation;

/**
 * Sets up which features of code generation are enabled.
 */
// TODO work in progress
// TODO: should "extra..." be instead "orphaned..."? Or something else?
public class CodeGenerationConfiguration {
	private boolean addMissingClasses = true; // eg: schema as new table
	private boolean addMissingProperties = true; // eg: schema has new column
	
	/**
	 * Causes classes to be deleted if they are annotated as mapping to tables
	 * that no longer exist.
	 */
	private boolean removeExtraClasses = true;
	
	/**
	 * Causes properties annotated as mapping to columns that no longer
	 * exist to be deleted.
	 * Annotated fields are deleted along with their accessor methods, regardless
	 * of whether the developer has made customisations to those fields or methods.
	 */
	private boolean removeExtraProperties = true;
	
	/**
	 * Causes code generation to stop on the first error encountered.
	 * This can be very troublesome when downloading the full schema is time consuming.
	 */
	private boolean stopOnErrors = false;
	
	// TODO nice to have: removeNonCustomisedExtraClasses
	// TODO nice to have: removeNonCustomisedExtraProperties
}
