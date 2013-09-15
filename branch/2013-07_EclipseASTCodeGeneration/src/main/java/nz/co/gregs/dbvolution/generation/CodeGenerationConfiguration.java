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
	
	/**
	 * Package path for generated classes that map to database tables.
	 */
	private String packageForTables;
	
	/**
	 * Package path for generated classes that map to database views.
	 */
	private String packageForViews;
	
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

	/**
	 * Gets the code generation package for tables and views.
	 * Convenience property for when it is desired that the tables and views are generated in the same package.
	 * @return the package if both tables and views set the same, null otherwise
	 */
	public String getPackage() {
		if (packageForTables != null && packageForViews != null && packageForTables.equals(packageForViews)) {
			return packageForTables; // both the same
		}
		return null;
	}

	/**
	 * Sets the code generation package for both tables and views.
	 * Convenience property for when it is desired that the tables and views are generated in the same package.
	 * @param package the package to set
	 */
	public void setPackage(String packageName) {
		this.setPackageForTables(packageName);
		this.setPackageForViews(packageName);
	}
	
	/**
	 * @return the packageForTables
	 */
	public String getPackageForTables() {
		return packageForTables;
	}

	/**
	 * @param packageForTables the packageForTables to set
	 */
	public void setPackageForTables(String packageForTables) {
		this.packageForTables = packageForTables;
	}

	/**
	 * @return the packageForViews
	 */
	public String getPackageForViews() {
		return packageForViews;
	}

	/**
	 * @param packageForViews the packageForViews to set
	 */
	public void setPackageForViews(String packageForViews) {
		this.packageForViews = packageForViews;
	}
	
}
