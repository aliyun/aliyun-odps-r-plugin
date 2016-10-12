source ./config.sh
VERSIONDATE=`date +"%Y-%m-%d %H:%M:%S"`
cat > ./RODPS/R/odps_version.R <<__EOF__
rodps.version <- function()
{
    print("RODPS ${RVERSION}")
    print("BUILDDATE ${VERSIONDATE}")
}
__EOF__
cat > ./RODPS/DESCRIPTION << __EOF__
Package: RODPS
Version: ${RVERSION}
Title: R interface to interact with ODPS
Description: This package is developed for R to interact with ODPS,
 which is the platform of Alibaba to process big data.
Author: mingchao.xiamc@alibaba-inc.com
License: Apache License 2.0
Depends: R (>= 1.8.0), rJava, DBI, RSQLite
Imports: methods
URL: http://gitlab.alibaba-inc.com/odps/rodps
Packaged: ${VERSIONDATE};  
__EOF__
