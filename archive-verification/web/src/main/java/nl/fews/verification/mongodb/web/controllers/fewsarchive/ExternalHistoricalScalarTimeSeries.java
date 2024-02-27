package nl.fews.verification.mongodb.web.controllers.fewsarchive;

import graphql.schema.DataFetchingEnvironment;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import nl.fews.verification.mongodb.web.shared.conversion.Conversion;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;

@Controller
public class ExternalHistoricalScalarTimeSeries {

	@QueryMapping
	public Document externalHistoricalScalarTimeSeriesById(@Argument String _id, DataFetchingEnvironment e){
		return  Mongo.findOne(Settings.get("archiveDB"), "ExternalHistoricalScalarTimeSeries", new Document("_id", new ObjectId(_id)), Conversion.getProjection(e));
	}
}
