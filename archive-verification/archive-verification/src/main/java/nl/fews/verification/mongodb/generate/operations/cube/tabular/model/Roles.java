package nl.fews.verification.mongodb.generate.operations.cube.tabular.model;

import nl.fews.verification.mongodb.generate.interfaces.IModel;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Roles implements IModel {
	private final Document template;

	public Roles(Document template){
		this.template = template;
	}

	@Override
	public void generate() {
		template.get("model", Document.class).append("roles", List.of(
			new Document("name", "Admin").append("modelPermission", "administrator").append("members", Arrays.stream(Settings.get("cubeAdmins").toString().split(",")).map(m -> new Document("memberName", m)).collect(Collectors.toList())),
			new Document("name", "User").append("modelPermission", "administrator").append("members", Arrays.stream(Settings.get("cubeUsers").toString().split(",")).map(m -> new Document("memberName", m)).collect(Collectors.toList()))));
	}
}
