### GPU Environment Variables
- `XLA_PYTHON_CLIENT_PREALLOCATE=false`
- `XLA_PYTHON_CLIENT_ALLOCATOR=platform`
### Weights
- https://noaa-nws-graphcastgfs-pds.s3.amazonaws.com/index.html#graphcastgfs_weights
- https://console.cloud.google.com/storage/browser/dm_graphcast
### Sources
- https://github.com/astral-sh/python-build-standalone/releases
- https://github.com/NOAA-EMC/graphcast
- https://github.com/google-deepmind/weathernext
### GIT CRLF to LF Windows
- `echo "* text=auto eol=lf" >> .gitattributes`
- `echo "*.cmd text eol=crlf" >> .gitattributes`
- `git add --renormalize .`
### PyCharm CRLF to LF Windows
- `Editor-CodeStyle-LineSeparator-LF`
### Security - WebProxy Self-Signed Certificate Issue Resolution
- Conda: `conda config --set ssl_verify false`
- GIT: `git config --global http.sslVerify false`
- PIP: `pip config set global.trusted-host "pypi.python.org pypi.org files.pythonhosted.org"`
