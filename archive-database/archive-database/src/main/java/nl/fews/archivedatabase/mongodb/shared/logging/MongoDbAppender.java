package nl.fews.archivedatabase.mongodb.shared.logging;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import nl.fews.archivedatabase.mongodb.shared.database.Collection;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.JsonLayout;
import org.bson.Document;

import java.net.InetAddress;
import java.util.Date;

@Plugin(name = "MongoDbAppender", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE)
public class MongoDbAppender extends AbstractAppender {

	/**
	 *
	 */
	private static MongoClient mongoClient = null;

	/**
	 *
	 */
	private static String collectionName = null;

	/**
	 *
	 */
	private static String databaseName = null;

	/**
	 *
	 */
	private static final JsonLayout jsonLayout =  JsonLayout.createDefaultLayout();

	/**
	 *
	 * @param name name
	 * @param filter filter
	 */
	protected MongoDbAppender(String name, Filter filter) {
		super(name, filter, jsonLayout, true, null);
	}

	/**
	 *
	 * @param name name
	 * @param connectionString connectionString
	 * @param filter filter
	 * @return MongoDbAppender
	 */
	@PluginFactory
	public static MongoDbAppender createAppender(
			@PluginAttribute("name") String name,
			@PluginAttribute("connectionString") String connectionString,
			@PluginElement("Filter") Filter filter) {
		MongoDbAppender.collectionName = Collection.MigrateLog.toString();
		MongoDbAppender.databaseName = new ConnectionString(connectionString).getDatabase();
		MongoDbAppender.mongoClient = MongoClients.create(connectionString);
		return new MongoDbAppender(name, filter);
	}

	/**
	 *
	 * @param event event
	 */
	@Override
	public void append(LogEvent event) {
		if(event.getLoggerName().startsWith("org.mongodb."))
			return;
		try{
			Document document = addExtraMessage(Document.parse(jsonLayout.toSerializable(event)));
			document.append("date", new Date());
			document.append("machineName", InetAddress.getLocalHost().getHostName());
			mongoClient.getDatabase(databaseName).getCollection(collectionName).insertOne(document);
		}
		catch (Exception ex){
			//IGNORE
		}
	}

	/**
	 *
	 * @param document document
	 */
	private Document addExtraMessage(Document document){
		try{
			Document extra = Document.parse(document.getString("message"));
			document.putAll(extra);
			if(!extra.containsKey("message"))
				document.remove("message");
			return document;
		}
		catch (Exception ex){
			return document;
		}
	}
}
