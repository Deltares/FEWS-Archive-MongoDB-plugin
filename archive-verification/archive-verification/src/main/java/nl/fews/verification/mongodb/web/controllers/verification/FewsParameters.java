package nl.fews.verification.mongodb.web.controllers.verification;

import graphql.schema.DataFetchingEnvironment;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.web.shared.conversion.Conversion;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;

import java.util.List;
import java.util.stream.StreamSupport;

@Controller
public class FewsParameters {
	@QueryMapping
	public Document fewsParametersById(@Argument String _id, DataFetchingEnvironment e){
		var r = Mongo.findOne("fews.Parameters", new Document("_id", new ObjectId(_id)), Conversion.getProjection(e));
		r.put("lastUpdated", Conversion.dateToOffsetDateTime(r.getDate("lastUpdated")));
		return r;
	}

	@QueryMapping
	public List<Document> fewsParametersN(DataFetchingEnvironment e){
		return StreamSupport.stream(Mongo.find("fews.Parameters", new Document(), Conversion.getProjection(e)).spliterator(), false).peek(r -> r.put("lastUpdated", Conversion.dateToOffsetDateTime(r.getDate("lastUpdated")))).toList();
	}
}
