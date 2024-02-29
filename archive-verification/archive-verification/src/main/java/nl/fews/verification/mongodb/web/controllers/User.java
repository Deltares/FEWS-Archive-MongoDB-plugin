package nl.fews.verification.mongodb.web.controllers;

import org.bson.Document;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.core.oidc.user.OidcUser;
import org.springframework.stereotype.Controller;

@Controller
public class User {
	@QueryMapping
	public Document user(){
		var user = ((OidcUser)SecurityContextHolder.getContext().getAuthentication().getPrincipal()).getUserInfo();
		return new Document("Name", user.getFullName()).append("Email", user.getEmail());
	}
}
