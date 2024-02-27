package nl.fews.verification.mongodb.generate.operations.cube.tabular.model;

import nl.fews.verification.mongodb.generate.interfaces.IModel;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.util.Arrays;
import java.util.List;

public class Roles implements IModel {
	private final Document template;

	public Roles(Document template){
		this.template = template;
	}

	/**
	 * This method generates the roles for a given model.
	 * It retrieves the template document from the model object.
	 * It creates two role documents - Admin and User.
	 * Each role document contains the name, modelPermission, and members fields.
	 * The members field is populated with member documents obtained from the settings.
	 * The settings are retrieved using the Settings class.
	 * The "cubeAdmins" and "cubeUsers" settings are split using a comma and converted to a stream.
	 * Each member name is mapped to a member document with the "memberName" field.
	 * The member documents are collected into a list and added to the respective role documents.
	 * Finally, the roles document is appended to the "model" field in the template document.
	 */
	@Override
	public void generate() {
		template.get("model", Document.class).append("roles", List.of(
			new Document("name", "Admin").append("modelPermission", "administrator").append("members", Arrays.stream(Settings.get("cubeAdmins").toString().split(",")).map(m -> new Document("memberName", m)).toList()),
			new Document("name", "User").append("modelPermission", "administrator").append("members", Arrays.stream(Settings.get("cubeUsers").toString().split(",")).map(m -> new Document("memberName", m)).toList())));
	}
}
