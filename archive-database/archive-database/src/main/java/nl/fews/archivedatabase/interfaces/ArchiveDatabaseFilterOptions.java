//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package nl.fews.archivedatabase.interfaces;

import java.util.Set;
import nl.wldelft.util.timeseries.TimeStep;

public interface ArchiveDatabaseFilterOptions {
    Set<String> getParameterIds();

    Set<String> getModuleInstanceIds();

    Set<TimeStep> getTimeSteps();
}
