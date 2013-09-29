/*
 * Copyright 2013 gregory.graham.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
	
	/**
	 * Causes generated annotations to be placed on the fields,
	 * and for the fields to be set public, instead of annotating
	 * the accessor methods.
	 * Note: if false, {@link #generateAccessorMethods} must be true.
	 */
	private boolean annotateFields = true;
	
	/**
	 * Whether to generate accessor methods or fields alone.
	 * Must be true if {@link #annotateFields} is false.
	 */
	private boolean generateAccessorMethods = true;

	// TODO nice to have: removeNonCustomisedExtraClasses
	// TODO nice to have: removeNonCustomisedExtraProperties
	
	// TODO: for maven plugin
//	/**
//	 * Sets the root output directory for generated code packages.
//	 * If null, the current working directory is used.
//	 */
//	private String outputDirectory = null;
	
	// TODO: for maven plugin
//	/**
//	 * Full qualified package name for generated classes that map to database tables and views.
//	 * Can be overridden individually for tables or views
//	 * by {@link #packageForTables} and {@link #packageForViews}.
//	 */
//	private String packageName;
	
	// TODO: for maven plugin
//	/**
//	 * Package path for generated classes that map to database tables.
//	 */
//	private String packageForTables;
	
	// TODO: for maven plugin
//	/**
//	 * Package path for generated classes that map to database views.
//	 */
//	private String packageForViews;

// TODO: for maven plugin
//	/**
//	 * Fully qualified class name of {@link nz.co.gregs.dbvolution.generation.PrimaryKeyRecognisor}.
//	 * Default: null.
//	 */
//	private String primaryKeyRecogniserClass;
	
// TODO: for maven plugin
//	/**
//	 * Fully qualified class name of {@link nz.co.gregs.dbvolution.generation.ForeignKeyRecognisor}.
//	 * Default: null.
//	 */
//	private String foreignKeyRecogniserClass;
	
	/**
	 * @return the addMissingClasses
	 */
	public boolean isAddMissingClasses() {
		return addMissingClasses;
	}

	/**
	 * @param addMissingClasses the addMissingClasses to set
	 */
	public void setAddMissingClasses(boolean addMissingClasses) {
		this.addMissingClasses = addMissingClasses;
	}

	/**
	 * @return the addMissingProperties
	 */
	public boolean isAddMissingProperties() {
		return addMissingProperties;
	}

	/**
	 * @param addMissingProperties the addMissingProperties to set
	 */
	public void setAddMissingProperties(boolean addMissingProperties) {
		this.addMissingProperties = addMissingProperties;
	}

	/**
	 * @return the removeExtraClasses
	 */
	public boolean isRemoveExtraClasses() {
		return removeExtraClasses;
	}

	/**
	 * @param removeExtraClasses the removeExtraClasses to set
	 */
	public void setRemoveExtraClasses(boolean removeExtraClasses) {
		this.removeExtraClasses = removeExtraClasses;
	}

	/**
	 * @return the removeExtraProperties
	 */
	public boolean isRemoveExtraProperties() {
		return removeExtraProperties;
	}

	/**
	 * @param removeExtraProperties the removeExtraProperties to set
	 */
	public void setRemoveExtraProperties(boolean removeExtraProperties) {
		this.removeExtraProperties = removeExtraProperties;
	}

	/**
	 * @return the stopOnErrors
	 */
	public boolean isStopOnErrors() {
		return stopOnErrors;
	}

	/**
	 * @param stopOnErrors the stopOnErrors to set
	 */
	public void setStopOnErrors(boolean stopOnErrors) {
		this.stopOnErrors = stopOnErrors;
	}

	/**
	 * @return the annotateFields
	 */
	public boolean isAnnotateFields() {
		return annotateFields;
	}

	/**
	 * @param annotateFields the annotateFields to set
	 */
	public void setAnnotateFields(boolean annotateFields) {
		this.annotateFields = annotateFields;
	}

	/**
	 * @return the generateAccessorMethods
	 */
	public boolean isGenerateAccessorMethods() {
		return generateAccessorMethods;
	}

	/**
	 * @param generateAccessorMethods the generateAccessorMethods to set
	 */
	public void setGenerateAccessorMethods(boolean generateAccessorMethods) {
		this.generateAccessorMethods = generateAccessorMethods;
	}

//	/**
//	 * Gets the code generation package for tables and views.
//	 * @return the package name for tables and views
//	 */
//	public String getPackageName() {
//		return packageName;
//	}
//
//	/**
//	 * Sets the code generation package for both tables and views.
//	 * Can be overridden individually by {@link #setPackageForTables(String)} ande
//	 * {@link #setPackageForViews(String)}.
//	 * @param package the package to set
//	 */
//	public void setPackageName(String packageName) {
//		this.packageName = packageName;
//	}
//	
//	/**
//	 * @return the packageForTables
//	 */
//	public String getPackageForTables() {
//		return (packageForTables == null) ? packageName : packageForTables;
//	}
//
//	/**
//	 * @param packageForTables the packageForTables to set
//	 */
//	public void setPackageForTables(String packageForTables) {
//		this.packageForTables = packageForTables;
//	}
//
//	/**
//	 * @return the packageForViews
//	 */
//	public String getPackageForViews() {
//		return (packageForViews == null) ? packageName : packageForViews;
//	}
//
//	/**
//	 * @param packageForViews the packageForViews to set
//	 */
//	public void setPackageForViews(String packageForViews) {
//		this.packageForViews = packageForViews;
//	}
//
//	/**
//	 * @return the outputDirectory
//	 */
//	public String getOutputDirectory() {
//		return outputDirectory;
//	}
//
//	/**
//	 * @param outputDirectory the outputDirectory to set
//	 */
//	public void setOutputDirectory(String outputDirectory) {
//		this.outputDirectory = outputDirectory;
//	}
}
