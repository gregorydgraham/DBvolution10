/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nz.co.gregs.dbvolution.generation;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.*;
import org.burningwave.core.assembler.ComponentContainer;
import org.burningwave.core.assembler.ComponentSupplier;
import static org.burningwave.core.assembler.StaticComponentContainer.Constructors;
import org.burningwave.core.classes.*;

/**
 *
 * @author gregorygraham
 */
public class DBRowSubclassGenerator {

	private DBRowSubclassGenerator() {
	}

	public static void generate(List<DBTableClass> dbTableClasses, Options options) {
		List<String> dbTableClassNames = new ArrayList<>(0);

		for (DBTableClass dbt : dbTableClasses) {
			dbTableClassNames.add(dbt.getClassName());
		}
		for (DBTableClass dbt : dbTableClasses) {
			linkForeignKeys(dbt, dbTableClassNames);
		}
		List<DBTableClass> compileClasses = new ArrayList<>(0);
		compileClasses.addAll(dbTableClasses);

		List<DBTableClass> rejectedClasses = new ArrayList<>(0);

		while (rejectedClasses.size() != compileClasses.size()) {
			rejectedClasses.clear();
			for (DBTableClass compileThis : compileClasses) {
				try {
					generateForClass(compileThis, options);
				} catch (Exception exc) {
					rejectedClasses.add(compileThis);
				}
			}
			compileClasses.clear();
			compileClasses.addAll(rejectedClasses);
		}
	}

	private static void linkForeignKeys(DBTableClass dbt, List<String> dbTableClassNames) {
		for (DBTableField dbf : dbt.getFields()) {
			if (dbf.isForeignKey) {
				if (!dbTableClassNames.contains(dbf.referencesClass)) {
					List<String> matchingNames = new ArrayList<>();
					for (String name : dbTableClassNames) {
						if (name.toLowerCase().startsWith(dbf.referencesClass.toLowerCase())) {
							matchingNames.add(name);
						}
					}
					if (matchingNames.size() == 1) {
						String properClassname = matchingNames.get(0);
						dbf.referencesClass = properClassname;
					}
				}
			}
		}
	}

	static final String DBRowPackageName = DBRow.class
			.getPackage().getName();

	private static void generateForClass(DBTableClass dbt, Options options) {
		UnitSourceGenerator generator = UnitSourceGenerator.create(dbt.getPackageName());

		var newDBRowClass = ClassSourceGenerator
				.create(TypeDeclarationSourceGenerator.create(dbt.getClassName()));

		if (dbt.getTableSchema() == null || "PUBLIC".equals(dbt.getTableSchema().toUpperCase()) || "dbo".equals(dbt.getTableSchema())) {
			newDBRowClass.addAnnotation(
					AnnotationSourceGenerator
							.create(DBTableName.class
							)
							.addParameter(
									VariableSourceGenerator
											.create("value")
											.setValue("\"" + dbt.getTableName() + "\"")
							)
			);
		} else {
			newDBRowClass.addAnnotation(
					AnnotationSourceGenerator
							.create(DBTableName.class
							)
							.addParameter(
									VariableSourceGenerator
											.create("value")
											.setValue("\"" + dbt.getTableName() + "\""),
									VariableSourceGenerator
											.create("schema")
											.setValue("\"" + dbt.getTableSchema() + "\"")
							)
			);
		}

		newDBRowClass.expands(DBRow.class
		)
				.addModifier(Modifier.PUBLIC)
				.addConstructor(FunctionSourceGenerator.create().addModifier(Modifier.PUBLIC).addBodyCode());
		newDBRowClass.addField(
				VariableSourceGenerator.create(Long.class,
						 "serialVersionUID")
						.addModifier(Modifier.PUBLIC).addModifier(Modifier.STATIC).addModifier(Modifier.FINAL)
						.setValue(options.getVersionNumber() + "L")
		);
		for (DBTableField field : dbt.getFields()) {
			VariableSourceGenerator newField = VariableSourceGenerator
					.create(field.columnType, field.fieldName)
					.addModifier(Modifier.PUBLIC)
					.setValue("new " + field.columnType.getSimpleName() + "()");
			if (field.comments == null || field.comments.isEmpty()) {
				newField.addAnnotation(
						AnnotationSourceGenerator.create(DBColumn.class
						)
								.addParameter(VariableSourceGenerator.create("value").setValue("\"" + field.columnName + "\""))
				);
			} else {
				newField.addAnnotation(
						AnnotationSourceGenerator.create(DBColumn.class
						)
								.addParameter(VariableSourceGenerator.create("value").setValue("\"" + field.columnName + "\""))
								.addParameter(VariableSourceGenerator.create("comments").setValue("\"" + field.comments.replaceAll("\"", "\\\"") + "\""))
				);
			}
			if (field.isPrimaryKey) {
				newField.addAnnotation(
						AnnotationSourceGenerator.create(DBPrimaryKey.class
						)
				);
			}
			if (field.isAutoIncrement) {
				newField.addAnnotation(
						AnnotationSourceGenerator.create(DBAutoIncrement.class
						)
				);
			}
			if (field.isForeignKey) {
				if (options.getIncludeForeignKeyColumnName()) {
					newField.addAnnotation(
							AnnotationSourceGenerator.create(DBForeignKey.class
							)
									.addParameter(VariableSourceGenerator.create("value").setValue(dbt.getPackageName() + "." + field.referencesClass + ".class"))
									.addParameter(VariableSourceGenerator.create("column").setValue("\"" + field.referencesField + "\""))
					);
				} else {
					newField.addAnnotation(
							AnnotationSourceGenerator.create(DBForeignKey.class
							)
									.addParameter(VariableSourceGenerator.create("value").setValue(dbt.getPackageName() + "." + field.referencesClass + ".class"))
					);
				}
			}
			if (dbt.getUnknownDatatype().equals(field.columnType)) {
				newField.addAnnotation(
						AnnotationSourceGenerator.create(DBUnknownJavaSQLType.class
						)
								.addParameter(VariableSourceGenerator.create("value").setValue("" + field.javaSQLDatatype))
				);
			}
			newDBRowClass.addField(newField);
		}

		generator.addClass(newDBRowClass);
		dbt.setJavaSource(generator.make());
		getClassFromGenerator(generator, dbt);

	}

	private static void getClassFromGenerator(UnitSourceGenerator generator, DBTableClass dbt) {
//		System.out.println("\nGenerated code:\n" + dbt.getJavaSource());
		//With this we store the generated source to a path
		generator.storeToClassPath(System.getProperty("user.home") + "/granity/temp");
		ComponentSupplier componentSupplier = ComponentContainer.getInstance();
		ClassFactory classFactory = componentSupplier.getClassFactory();
		//this method compile all compilation units and upload the generated classes to default
		//class loader declared with property "class-factory.default-class-loader" in 
		//burningwave.properties file (see "Overview and configuration").
		//If you need to upload the class to another class loader use
		//loadOrBuildAndDefine(LoadOrBuildAndDefineConfig) method
		ClassFactory.ClassRetriever classRetriever = classFactory.loadOrBuildAndDefine(generator);
		@SuppressWarnings("unchecked")
		Class<? extends DBRow> generatedClass = (Class<? extends DBRow>) classRetriever.get(dbt.getFullyQualifiedName());
		dbt.setGeneratedClass(generatedClass);
		DBRow generatedClassObject = Constructors.newInstanceOf(generatedClass);
		dbt.setGeneratedInstance(generatedClassObject);
	}
}
