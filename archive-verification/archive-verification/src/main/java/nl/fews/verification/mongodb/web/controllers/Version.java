package nl.fews.verification.mongodb.web.controllers;

import nl.fews.verification.mongodb.web.Application;
import org.bson.Document;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;

@Controller
public class Version {
	@QueryMapping
	public Document version(){
		var version = Application.class.getPackage().getImplementationVersion();
		return new Document("Version", version);
	}
}
