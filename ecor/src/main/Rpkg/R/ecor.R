#' R wrapper for running ecor queries etc. using rJava
#'  
#' to enable running R on task nodes:
#' 
#' \enumerate{
#' \item{ install R } 
#' \item{ \code{install.packages("rJava")}}
#' \item{ configure R_HOME variable in the mapred user env (or whichever user runs the tasks)
#'   on Ubuntu it is usually /usr/lib/R }
#' \item{ symbolic-link /lib64/libjri.so -> /usr/local/lib/R/site-library/rJava/jri/libjri.so
#'   (or otherwise make sure libjri.so is available thru java.library.path at the time 
#'    of MR task loading) }
#' }
#' \strong{Tip:} The following command should produce location dir of libjri.so:
#' \code{R --vanilla <<< 'system.file("jri", package="rJava")'}
#' 
#' @docType package
#' @name ecor
#' @author: dmitriy
NULL


##########################
# generic initialization #
##########################

.onLoad <- function (libname=NULL,pkgname=NULL) .ecor.init(libname,pkgname, pkgInit=T)
.onUnload <- function(libpath) rm(ecor) 

.ecor.init <- function(libname=NULL, pkgname=NULL, pkgInit = F) {
	
	library(rJava)
	
	if ( length(pkgname) == 0 ) pkgname <- "ecor"
	
	ecor <- list()
	
	hadoopcp <- ecor.hadoopClassPath()
	
	if ( pkgInit ) {
		.jpackage(pkgname, morePaths = hadoopcp, lib.loc = libname)
		cp <- list.files(system.file("java",package=pkgname,lib.loc=libname),
				full.names=T, pattern ="\\.jar$")
		ecor$cp <- cp
	} else {
		# DEBUG mode: package not installed.
		# look files in a maven project tree 
		# denoted by ECO_HOME
		ecoHome <- Sys.getenv("ECO_HOME")
		if ( nchar(ecoHome)==0 )
			stop ("for initializing from maven tree, set ECO_HOME variable.")
		
		libdir <- file.path ( ecoHome, "target")
		pkgdir <- list.files(libdir, pattern= "^ecor-.*-rpkg$", full.names=T)
		cp <- c ( list.files( file.path(pkgdir, "inst", "java"),pattern="\\.jar$",full.names=T),
				file.path(libdir,"test-classes"))
		.jinit(classpath = c(hadoopcp,cp))
		
		ecor$cp <- cp
		
	}
	
	# make sure all classpath entries exists, 
	# it may cause problems later.
	ecor$cp <- ecor$cp[file.exists(ecor$cp)]
	
	ecor$jconf <- new(J("org.apache.hadoop.conf.Configuration"))
	
	consts <- character(0)
	# tight integration with some hadoop stuff to bypass the need 
	# to have that stuff in classpath and actually do wrapper java calls.
	
	consts["INPUT_FORMAT"] <- "mapreduce.inputformat.class"
	consts["OUTPUT_FORMAT"] <- "mapreduce.outputformat.class"
	consts["MAP"] <- "mapreduce.map.class"
	consts["COMBINE" ] <- "mapreduce.combine.class"
	consts["REDUCE" ] <- "mapreduce.reduce.class"
	consts["PARTITION"] <- "mapreduce.partitioner.class"
	consts["NAME"] <- "mapred.job.name"
	consts["INPUT"] <- "mapred.input.dir"
	consts["OUTPUT"] <- "mapred.output.dir"
	
	
	ecor$consts <- consts
	
	ecor <<- ecor
}


#' Discover hadoop class path
#' 
#' Discover hadoop classpath based on HADOOP_HOME environment variable.
#' 
#' @author dmitriy
ecor.hadoopClassPath <- function () {
	hhome <- Sys.getenv("HADOOP_HOME")
	
	if ( nchar(hhome) ==0 )
		stop ("HADOOP_HOME not set")
	
	hlibdir <- file.path (hhome, "lib")
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

#' Produce local hbase path
#' 
#' Produce local hbase path
#' 
#' @author dmitriy
ecor.hBaseClassPath <- function () {
	hhome <- Sys.getenv("HBASE_HOME")
	
	if ( nchar(hhome) ==0 )
		stop ("HBASE_HOME not set")
	
	hlibdir <- file.path (hhome, "lib")
	if ( ! file.exists(hlibdir))
		stop ( sprintf("cannot find %s directory.", hlibdir))
	
	
	hbaselib <- list.files(
			hlibdir,
			full.names = T,
			pattern="\\.jar$")
	
	hbasecore <- list.files (
			hhome,
			full.names=T,
			pattern=".*core.*\\.jar"
	)
	
	c(hbaselib,hbasecore)
}


ecor.pigClassPath <- function () {
	hhome <- Sys.getenv("PIG_HOME")
	
	if ( nchar(hhome) ==0 )
		stop ("PIG_HOME not set")
	
	hlibdir <- file.path (hhome, "lib")
	if ( ! file.exists(hlibdir))
		stop ( sprintf("cannot find %s directory.", hlibdir))
	
	
	piglib <- list.files(
			hlibdir,
			full.names = T,
			pattern="\\.jar$")
	
	pigcore <- list.files (
			hhome,
			full.names=T,
			pattern=".*core.*\\.jar"
	)
	
	c(piglib,pigcore)
}


##################################
# generic MR job driver          #
##################################

# convert hadoop Configuration to a 
# character vector with string subscripts 
# corresponding to prop names 
# Configuration is Iterable<Map.Entry<String,String>>
.ecor.jconf2hconf <- function (jconf) {
	conf <- ecor.hconf()
	iter <- jconf$iterator()
	while (iter$hasNext() ) {
		map.entry <- iter$"next"()
		conf[as.character(map.entry$getKey())] <- as.character(map.entry$getValue())
	}
	conf
}

# convert character vector to configuration
.ecor.hconf2jconf <- function (v ) {
	jconf <- new (J("org.apache.hadoop.conf.Configuration"),ecor$jconf)
	sapply(names(v), function(n) jconf$set(n,v[n]))
	jconf
}

# convert string to Path 
.ecor.jpath <- function(parent, child = NULL) {
	if ( length(child) == 0 )
		new ( J("org.apache.hadoop.fs.Path"), parent )
	else 
		new ( J("org.apache.hadoop.fs.Path"),
				.ecor.jpath(parent),child)
}

#local fs
.ecor.localFS <- function () {
	J("org.apache.hadoop.fs.FileSystem")$getLocal(ecor$jconf)
}

ecor.hconf <- function() { 
	a <- character(0); class(a) <- "hconf";
	
	# set some defaults 
	ecor.map(a) <- "com.inadco.ecoadapters.r.RMapper"
	ecor.inputFormat(a) <- "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat"
	ecor.outputFormat(a) <- "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat"
	
	a
}

.ecor.toB64 <- function(x) {
	rawx <- serialize(x, NULL, ascii = F)
	rawToChar(J("org.apache.commons.codec.binary.Base64")$encodeBase64(.jarray(rawx)))
}
.ecor.fromB64 <- function (x) {
	rawx <- J("org.apache.commons.codec.binary.Base64")$decodeBase64(.jarray(charToRaw(x)))
	rx <- unserialize(rawx)
}

ecor.hjob <- function(x,...) UseMethod("hjob")
"ecor.inputFormat<-" <- function(x,...) UseMethod("inputFormat<-")
ecor.inputFormat <- function(x,...) UseMethod("inputFormat")
"ecor.outputFormat<-" <- function(x,...) UseMethod("outputFormat<-")
ecor.outputFormat <- function(x,...) UseMethod("outputFormat")
"ecor.map<-" <- function(x,...) UseMethod("map<-")
ecor.map <- function(x,...) UseMethod("map")
"ecor.reduce<-" <- function(x,...) UseMethod("reduce<-")
ecor.reduce <- function(x,...) UseMethod("reduce")

"inputFormat<-.hconf" <- function(x, value) { x[ecor$consts["INPUT_FORMAT"]] <- value; x} 
inputFormat.hconf <- function (x) x[ecor$consts["INPUT_FORMAT"]]
"outputFormat<-.hconf" <- function(x, value) { x[ecor$consts["OTUPUT_FORMAT"]] <- value; x}
outputFormat.hconf <- function (x ) x[ecor$consts["OUTPUT_FORMAT"]]
"map<-.hconf" <- function(x, value) { x[ecor$consts["MAP"]] <- value; x }
map.hconf <- function(x) x[ecor$consts["MAP"]]
"reduce<-.hconf" <- function(x, value ) { x[ecor$consts["REDUCE"]] <- value; x }
reduce.hconf <- function (x) x[ecor$consts["REDUCE"]]


"ecor.input<-" <- function (x,...) UseMethod("input<-")
ecor.input <- function (x,...) UseMethod("input")
"ecor.output<-" <- function (x,...) UseMethod("output<-")
ecor.output <- function (x,...) UseMethod("output")

"input<-.hconf" <- function (x, value) { x[ecor$consts["INPUT"]] <- value; x}
input.hconf <- function (x) x[ecor$consts["INPUT"]]
"output<-.hconf" <- function(x,value) { x[ecor$consts["OUTPUT"]] <- value; x}
output.hconf <- function (x) x[ecor$consts["OUTPUT"]]

#actually create job handle and submit
hjob.hconf <- function(conf, MAPFUN, REDUCEFUN = NULL ) {
	
	if ( class(MAPFUN) != "function" )
		stop ("mapper R function expected.")
	
	
	conf["ecor.NAMESPACES"] <- .ecor.toB64(loadedNamespaces())
	
	mapfunfilename <- tempfile()
	f <- file(mapfunfilename, open="wb")
	tryCatch({
				# my tests seem to indicate 
				# that this serializes all the function 
				# environment too.
				serialize(MAPFUN, f, ascii = F)
			},
			finally = close(f)
	)
	
	tryCatch ({
				conf["ecor.MAPFUN"] <- basename(mapfunfilename)
				
				reducefunfilename <- NULL 
				if ( length(REDUCEFUN) >0  ) {
					reducefunfilename <- tempfile()
					f <- file(reducefunfilename, open="wb")
					tryCatch({
								serialize(REDUCEFUN,f,ascii = F)
							},
							finally = {
								close(f)
							})
					conf["ecor.REDUCEFUN"] <- basename(reducefunfilename)
				}
				
				jconf <- .ecor.hconf2jconf(conf)
				
				# broadcast tempfile containing environment
				J("org.apache.hadoop.filecache.DistributedCache")$
				addCacheFile( new(J("java.net.URI"),mapfunfilename), jconf)
				
				# pre-0.23 way of doing this 
				sapply(ecor$cp[!file.info(ecor$cp)[,"isdir"]], 
						function(f)	J("org.apache.hadoop.filecache.DistributedCache")$
							addFileToClassPath(.ecor.jpath(f), jconf, .ecor.localFS()),
						simplify=T)
				
				hjob <- new (J("org.apache.hadoop.mapreduce.Job"),jconf)
				hjob$submit() 
				
				file.remove(mapfunfilename)
				mapfunfilename <- NULL
				file.remove(reducefunfilename)
				reducefunfilename <- NULL
				
			}, 
			finally = {
				if ( length(mapfunfilename) > 0 )  file.remove(mapfunfilename)
				if ( length(reducefunfilename) > 0 ) file.remove(reducefunfilename)
			}
	)
	
}



##################################
# Generic mapper configuration   #
##################################

.ecor.collectbuff <- function () {
	buff <- list()
	buff$size <- 0
	buff$keys <- list()
	buff$values <- list()
	class(buff) <- "collectbuff"
	buff
}


.ecor.tasksetup <- function ( jconf, jcontext, mapsetup=T ) {
	
	conf <- .ecor.jconf2hconf(jconf)
	
	# frontend packages translated to backend 
	# to load here as well.
	packages <- conf['ecor.NAMESPACES']
	if ( packages == NULL ) 
		stop ("no packages in the job configuration")
	
	packages <- .ecor.fromB64(packages)
	
	
	require(packages)
	
	ecor$conf <<- conf
	
	filePaths <- as.list(J("org.apache.hadoop.filecache.DistributedCache")$getLocalCacheFiles())
	filePaths <- sapply(filePaths, function(x) x$toString() )
	names(filePaths) <- sapply(filePaths, function(x) basename(x))
	
	if ( mapsetup ) {
		mapfunfilename <- conf['ecor.MAPFUN']
		mapfunfilepath <- filePaths[[mapfunfilename]]
		if ( mapfunfilepath == NULL ) 
			stop ("Unable to locate map function file path in the distributed cache files.")
		
		MAPFUN <<- NULL
		f <- file(mapfunfilepath, "rb")
		tryCatch({
					MAPFUN <<- unserialize(f)
				},
				finally = {
					close(f)
				})
		
	} else {
		# TODO: reduce task setup.
	}
	
	collectbuff <<- ecor.collectbuff()
}

.ecor.maptask <- function (jconf, jcontext, jkey, jvalue ) {
	
}


# to be called internally by protobuf mapper 
.ecor.protomaptask <- function ( key, msgBytes ) {
	value <- proto.fromProtoRaw( msgBytes, ecor$d )
	collectbuff <<- .ecor.collectbuff()
	tryCatch({
				MAPFUN(key,value )
				collectbuff
			},
			finally = { 
				rm(collectbuff) 
			})
}

.ecor.reducetask <- function ( key, valueVector ) {
	values <- as.list(valueVector)
	collectbuff <<- .ecor.collectbuff
	tryCatch({
				REDUCEFUN(key,jmapvalue )
				collectbuff
			},
			finally = { 
				rm(collectbuff) 
			})
}

ecor.collect <- function (key, value) {
	
	collectbuff$keys[[ collectbuffsize ]] <- switch(mode(key), 
			numeric=key,
			integer=key,
			character=key,
			serialize(key,NULL))
	
	collectbuff$values[[ collectbuff$size ]] <- serialize(value,NULL)
	collectbuff$size <- collectbuff$size + 1 
	
}

