# TODO: R wrapper for running hbl queries etc. using rJava
# 
# Author: dmitriy
###############################################################################

library("rJava");

##########################
# generic initialization #
##########################

# we assume that hadoop, hbase and HBL 
# homes are required and PIG_HOME is 
# optional (but if pig is not installed 
# then compiler functions will not work
# in this session).
ecor.init <- function () {
	
	hbl <- list()
	hbl$options <- list()
	hbl$consts <- list()
	hbl$consts$HBASE_CONNECTION_PER_CONFIG <- "hbase.connection.per.config"
	
    hbl$options$HBL_HOME <- Sys.getenv("HBL_HOME");
	hbl$options$HADOOP_HOME <- Sys.getenv("HADOOP_HOME");
	hbl$options$HBASE_HOME <- Sys.getenv("HBASE_HOME");
	hbl$options$PIG_HOME <- Sys.getenv("PIG_HOME");
	
	if ( nchar(hbl$options$HADOOP_HOME) ==0 )
		stop ("HADOOP_HOME not set");
	
	if ( nchar(hbl$options$HBL_HOME) == 0 ) 
		stop("HBL_HOME not set");
	
	if ( nchar(hbl$options$HBASE_HOME)==0 ) 
		stop("HBASE_HOME not set");
	
	
	
	
	cp1 <- list.files(
			paste( hbl$options$HADOOP_HOME, "lib", sep="/" ), 
			full.names = T,
			pattern="\\.jar$")
	
	core <- list.files (
			hbl$options$HADOOP_HOME,
			full.names=T,
			pattern=".*core.*\\.jar"
			)
	
	cp2 <- list.files (
			paste(hbl$options$HBL_HOME, "lib", sep="/"),
			full.names = T,
			pattern="\\.jar$"
			)
		
	cp3 <- character(0)
	
	for ( assemblyDir in list.files (
			paste(hbl$options$HBL_HOME,"hbl/target",sep="/"),
			full.names=T,
			pattern="hbl-.*-dist$",
			include.dirs=T
			)) {  
				
		cp3 <- c(cp3, list.files(
						paste(assemblyDir, "lib", sep="/"),
						full.names=T,
						pattern="\\.jar$"
						))
	}
	
	pigcp <- if ( length(hbl$PIG_HOME)>0 ) 
		list.files(
				paste(hbl$PIG_HOME,"lib",sep="/"),
				full.names=T,
				pattern = "\\.jar$"
				)
	
	
	hb_core <- list.files (
			hbl$options$HBASE_HOME,
			full.names=T,
			pattern=".*hbase.*\\.jar")
	
	# TODO: pig classpath, too?
	
	hconf <- Sys.getenv("HADOOP_CONF")
	
	if (hconf == "")
		hconf <- paste(hbl$options$HADOOP_HOME,"conf",sep="/")
	
	hbaseConf <- paste(hbl$options$HBASE_HOME,"conf",sep="/")
	

  	hbl$classpath <- c(cp1,cp2,cp3,core,hb_core, hconf,hbaseConf, pigcp)
	
	.jinit(classpath = hbl$classpath )	

	hbl$conf <- new(J("org.apache.hadoop.conf.Configuration"))
	hbl$conf <- J("org.apache.hadoop.hbase.HBaseConfiguration")$create(hbl$conf)
	
	# chd3u3 or later requred
	hbl$conf$setBoolean(hbl$consts$HBASE_CONNECTION_PER_CONFIG,F)
	
	hbl$queryClient <- new(J("com.inadco.hbl.client.HblQueryClient"),hbl$conf)
	
	hbl <<- hbl
	
}

.checkInit <- function() if ( !exists(hbl) ) hbl.init()

.checkPig <- function() { 
	.checkInit()
	if ( length(hbl$PIG_HOME) == 0 )
		stop("pig access is not initialized in this session (have you set PIG_HOME?)")
}

