//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package nl.fews.archivedatabase.interfaces;

import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseRegionConfigInfoProvider;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseTimeConverter;
import nl.wldelft.fews.system.data.externaldatasource.archivedatabase.ArchiveDatabaseUnitConverter;
import nl.wldelft.util.Properties;

public interface ArchiveDatabaseFewsEnvironmentSettings {
    void setUnitConverter(ArchiveDatabaseUnitConverter unitConverter);

    void setTimeConverter(ArchiveDatabaseTimeConverter timeConverter);

    void setRegionConfigInfoProvider(ArchiveDatabaseRegionConfigInfoProvider databaseRegionConfigInfoProvider);

    void setProperties(Properties properties);

    void setConfigRevision(String configRevision);
}
