### FEWS
- Database archive plugin using MongoDB developed for Deltares FEWS by INFISYS Inc.
### JVM Configuration
- Startup Arguments
  - `-Djavax.net.ssl.trustStore=NONE`
  - `-Djavax.net.ssl.trustStoreType=Windows-ROOT`
  - `-Xms4g -Xmx40g`
### MongoDB Configuration
- `db.adminCommand({setParameter: 1, "internalQueryPlannerGenerateCoveredWholeIndexScans": true});`