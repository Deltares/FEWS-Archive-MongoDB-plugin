package nl.fews.verification.mongodb.generate.interfaces;

public interface IModel {
	/**
	 * Generates the necessary fields, data sources, tables, and roles for verification.
	 *
	 * This method is used to execute the verification process for a given study. It performs the following steps:
	 * 1. Retrieves the study document from the database using the provided study name.
	 * 2. Retrieves the template document from the database using the cube name from the study document.
	 * 3. Appends the necessary fields to the template document for verification.
	 * 4. Generates the data sources, tables, and roles for the verification using the template document.
	 * 5. Inserts the study and the template document into the output.Cube collection.
	 */
	void generate();
}
