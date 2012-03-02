# TODO: R wrapper for running ecor queries etc. using rJava
# 
# Author: dmitriy
###############################################################################

library("rJava");

##########################
# generic initialization #
##########################

.onLoad <- function (libname,pkgname) .init(libname,pkgname, pkgInit=T)
.onUnload <- function(libpath) rm(ecor) 

.ecor.init <- function(libname = NULL, pkgname = NULL, pkgInit = F) {

	hadoopcp <- .ecor.hadoopClassPath()
	
	if ( pkgInit ) {
		.jpackage(pkgname,morePaths=hadoopcp)
	} else {
		# DEBUG mode: package not installed.
		# look files in a maven project tree 
		# denoted by ECO_HOME
		ecoHome <- Sys.getenv("ECO_HOME")
		if ( nchar(ecoHome)==0 )
			stop ("for initializing from maven tree, set ECO_HOME variable.")
		
		libdir <- paste ( ecoHome, "ecor/target","/")
		libdir <- list.files(libdir, pattern= "^ecor-.*-rpkg$", full.names=T)
		cp <- list.files(paste(libdir,"java","/"),pattern="\\.jar$",full.names=T)
		.jinit(classpath=c(hadoopcp,cp))
		
	}
	
	ecor <- list()
	ecor$conf <- new(J("org.apache.hadoop.conf.Configuration"))
	ecor <<- ecor
}

.ecor.hadoopClassPath <- function () {
	hhome <- Sys.getenv("HADOOP_HOME")
	
	if ( nchar(hhome) ==0 )
		stop ("HADOOP_HOME not set")
	
	hlibdir <- paste (hhome, "lib", sep="/")
	if ( ! file.exists(hlibdir))
		stop ( sprintf("cannot find %s directory.", hlibdir))
	
	
	hadooplib <- list.files(
			hlibdir,
			full.names = T,
			pattern="\\.jar$")
	
	hadoopcore <- list.files (
			hhome,
			full.names=T,
			pattern=".*core.*\\.jar"
	)
	
	c(hadooplib,hadoopcore)
}

.checkInit <- function() if ( !exists(ecor) ) ecor.init()


