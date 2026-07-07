package nl.fews.archivedatabase.common.shared.database;

import nl.fews.archivedatabase.common.shared.interfaces.DatabaseBridge;
import nl.fews.archivedatabase.common.shared.settings.Settings;

import java.lang.reflect.InvocationTargetException;

/**
 *
 */
public final class Database {
	/**
	 *
	 */
	private static volatile DatabaseBridge instance = null;

	/**
	 *
	 */
	private Database(){}

	/**
	 *
	 * @return Database
	 */
	public static DatabaseBridge instance(){
		try {
			if (instance == null)
				synchronized (Database.class) {
					if (instance == null)
						instance = (DatabaseBridge) Class.forName(String.format(Settings.DATABASE_NAMESPACE, Settings.get("databaseType", String.class).toLowerCase())).getConstructor().newInstance();
				}
		}
		catch (ClassCastException | ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e){
			throw new RuntimeException(e);
		}
		return instance;
	}
}
