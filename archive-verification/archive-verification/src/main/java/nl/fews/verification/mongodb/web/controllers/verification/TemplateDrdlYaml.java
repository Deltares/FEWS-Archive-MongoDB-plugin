package nl.fews.verification.mongodb.web.controllers.verification;

import graphql.schema.DataFetchingEnvironment;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.web.shared.conversion.Conversion;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Controller
public class TemplateDrdlYaml {
	@QueryMapping
	public Document templateDrdlYamlById(@Argument String _id, DataFetchingEnvironment e){
		return  Mongo.findOne("template.DrdlYaml", new Document("_id", new ObjectId(_id)), Conversion.getProjection(e));
	}

	@QueryMapping
	public Document templateDrdlYamlByDatabaseTypeName(@Argument String database, @Argument String type, @Argument String name, DataFetchingEnvironment e){
		return  Mongo.findOne("template.DrdlYaml", new Document("Database", database).append("Type", type).append("Name", name), Conversion.getProjection(e));
	}

	@QueryMapping
	public List<Document> templateDrdlYamlN(DataFetchingEnvironment e){
		return StreamSupport.stream(Mongo.find("template.DrdlYaml", new Document(), Conversion.getProjection(e)).spliterator(), false).collect(Collectors.toList());
	}
	
	@MutationMapping
	public String createTemplateDrdlYaml(@Argument Map<String, Object> document){
		return Mongo.insertOne("template.DrdlYaml", new Document(document)).getInsertedId().asObjectId().getValue().toString();
	}

	@MutationMapping
	public Long updateTemplateDrdlYaml(@Argument String _id, @Argument Map<String, Object> document){
		return Mongo.updateOne("template.DrdlYaml", new Document("_id", new ObjectId(_id)), new Document(document)).getModifiedCount();
	}

	@MutationMapping
	public Long deleteTemplateDrdlYaml(@Argument String _id){
		return Mongo.deleteOne("template.DrdlYaml", new Document("_id", new ObjectId(_id))).getDeletedCount();
	}
}
