package nl.fews.archivedatabase.mongodb.migrate.utils;

import nl.wldelft.archive.util.metadata.netcdf.NetcdfMetaData;
import nl.wldelft.archive.util.metadata.simulation.SimulationMetaData;
import nl.wldelft.archive.util.runinfo.ArchiveRunInfo;
import nl.wldelft.archive.util.runinfo.ArchiveRunInfoReader;

import java.io.File;
import java.io.IOException;

/**
 *
 */
public final class RunInfoUtil {

	/**
	 * Static Class
	 */
	private RunInfoUtil(){}

	/**
	 * @param netcdfMetaData netcdfMetaData
	 * @return JSONObject
	 */
	public static ArchiveRunInfo getRunInfo(NetcdfMetaData netcdfMetaData) {
		if(!(netcdfMetaData instanceof SimulationMetaData))
			return null;
		try{
			File runInfoFile = ((SimulationMetaData)netcdfMetaData).getRunInfoFile();
			if(runInfoFile.exists()){
				try (ArchiveRunInfoReader archiveRunInfoReader = new ArchiveRunInfoReader(runInfoFile)) {
					return archiveRunInfoReader.getRunInfo();
				}
			}
			else {
				return null;
			}
		}
		catch (IOException ex){
			throw new RuntimeException(ex);
		}
	}
}