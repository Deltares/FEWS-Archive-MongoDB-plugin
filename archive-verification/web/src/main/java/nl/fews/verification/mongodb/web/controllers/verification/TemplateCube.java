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
import java.util.stream.StreamSupport;

@Controller
public class TemplateCube {
	@QueryMapping
	public Document templateCubeById(@Argument String _id, DataFetchingEnvironment e){
		return Mongo.findOne("template.Cube", new Document("_id", new ObjectId(_id)), Conversion.getProjection(e));
	}

	@QueryMapping
	public Document templateCubeByName(@Argument String name, DataFetchingEnvironment e){
		return Mongo.findOne("template.Cube", new Document("Name", name), Conversion.getProjection(e));
	}

	@QueryMapping
	public List<Document> templateCubeN(DataFetchingEnvironment e){
		return StreamSupport.stream(Mongo.find("template.Cube", new Document(), Conversion.getProjection(e)).spliterator(), false).toList();
	}
	
	@MutationMapping
	public String createTemplateCube(@Argument String name, @Argument Map<String, Object> template){
		return Mongo.insertOne("template.Cube", new Document("Name", name).append("Template", template)).getInsertedId().asObjectId().getValue().toString();
	}

	@MutationMapping
	public Long updateTemplateCube(@Argument String _id, @Argument String name, @Argument Map<String, Object> template){
		return Mongo.updateOne("template.Cube", new Document("_id", new ObjectId(_id)), new Document("$set", new Document("Name", name).append("Template", template))).getModifiedCount();
	}

	@MutationMapping
	public Long deleteTemplateCube(@Argument String _id){
		return Mongo.deleteOne("template.Cube", new Document("_id", new ObjectId(_id))).getDeletedCount();
	}
}
