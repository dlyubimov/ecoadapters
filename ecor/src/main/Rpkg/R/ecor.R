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
#' @exportPattern "^ecor\\.|^proto\\."
#' @import rJava
#' @include protobuf.R
#' @include sequenceFile.R
NULL


##########################
# generic initialization #
##########################

.onLoad <- function (libname=NULL,pkgname=NULL) .ecor.init(libname,pkgname, pkgInit=T)
.onUnload <- function(libpath) rm(ecor) 

.ecor.init <- function(libname=NULL, pkgname=NULL, pkgInit = F) {
	
	library(rJava)
	
	if ( length(pkgname) == 0 ) pkgname <- "ecor"
	
	ecor <- new.env()
	
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
#' Warning: this is very much ad-hoc and assumes current CDH 
#' hadoop layout for hadoop libs. This probably needs to be 
#' re-done according to the best practices for Hadoop stuff.
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
	# ensure config is loaded form 
	# the client dir 
	hadoopconf <- file.path(hhome,"conf")
	if ( ! file.exists(hadoopconf) )
		stop ("Unable to find hadoop configuration files.")
	
	c(hadooplib, hadoopcore, hadoopconf)
}

#' Produce local hbase path
#' 
#' Produce local hbase path
#' 
#' TODO: re-do per best practices, too
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
			pattern="^hbase-.*\\.jar"
	)
	hbconf <- file.path(hhome,"conf")
	if ( ! file.exists(hadoopconf) )
		stop ("Unable to find hbase configuration files.")
	
	c(hbaselib,hbasecore,hbconf)
}

#' Detect pig classpath.
#' 
#' stop if not found (looking for PIG_HOME).
#'  
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

#' initialize new HConf instance.
#' 
#' @method initialize HConf
#' @param jconf rJava reference to \code{o.a.h.conf.Configuration}
initialize.HConf <- function (jconf=NULL) {
	iter <- jconf$iterator()
	props <<- character(0)
	if ( length(jconf)>0) 
	while (iter$hasNext() ) { 
		map.entry <- iter$`next`()
		props[as.character(map.entry$getKey())] <<- as.character(map.entry$getValue())
	}
	props[ecor$consts["MAP"]] <<- "com.inadco.ecoadapters.r.RMapper"
	props[ecor$consts["INPUT_FORMAT"]] <<- "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat"
	props[ecor$consts["OUTPUT_FORMAT"]] <<- "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat"

}

#' convert to rJava \code{o.a.h}
#' 
#' convert to rJava reference \code{o.a.h.conf.Configuration} instance, 
#' also merge with the default hadoop configuration values.
#' 
#' @method as.jconf HConf
as.jconf.HConf <- function () { 
	jconf <- new (J("org.apache.hadoop.conf.Configuration"), ecor$jconf)
	for ( n in names(props) )jconf$set(n,props[n])
	jconf
}

#' set conf prop
#' 
#' @method set HConf
set.HConf <- function (name, val) {
	props[name] <<- val
	NULL
}

#' get conf prop
#' 
#' @method get HConf
get.HConf <- function (name) props[name]

setInputFormat.HConf <- function(value) props[ecor$consts["INPUT_FORMAT"]] <<- value 
getInputFormat.HConf <- function () props[ecor$consts["INPUT_FORMAT"]]
setOutputFormat.HConf <- function( value) props[ecor$consts["OUTPUT_FORMAT"]] <<- value
getOutputFormat.HConf <- function ( ) props[ecor$consts["OUTPUT_FORMAT"]]
setMapper.HConf <- function(value) props[ecor$consts["MAP"]] <<- value
getMapper.HConf <- function () props[ecor$consts["MAP"]]
setReducer.HConf <- function (value) props[ecor$consts["REDUCE"]] <<- value
getReducer.HConf <- function () props[ecor$consts["REDUCE"]]


setInput.HConf <- function (value) props[ecor$consts["INPUT"]] <<- value
getInput.HConf <- function () props[ecor$consts["INPUT"]]
setOutput.HConf <- function(value) props[ecor$consts["OUTPUT"]] <<- value
getOutput.HConf <- function () props[ecor$consts["OUTPUT"]]

mrSubmit.HConf <- function (MAPFUN, REDUCEFUN=NULL) ecor.HJob$new(.self,MAPFUN, REDUCEFUN )

#' R5 class holding MR configuration etc. stuff.
#' 
#' 
ecor.HConf <- setRefClass("HConf", 
		fields=list(props="list"),
		methods=list(
				initialize =initialize.HConf,
				as.jconf =as.jconf.HConf,
				set = set.HConf,
				get = get.HConf,
				setInputFormat = setInputFormat.HConf,
				getInputFormat = getInputFormat.HConf,
				setOutputFormat = setOutputFormat.HConf,
				getOutputFormat = getOutputFormat.HConf,
				setMapper = setMapper.HConf,
				getMapper = getMapper.HConf,
				setReducer = setReducer.HConf,
				getReducer = getReducer.HConf,
				setInput = setInput.HConf,
				getInput = getInput.HConf,
				mrSubmit = mrSubmit.HConfs
				))
		
##################################
# generic MR job driver          #
##################################


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

.ecor.toB64 <- function(x) {
	rawx <- serialize(x, NULL, ascii = F)
	rawToChar(J("org.apache.commons.codec.binary.Base64")$encodeBase64(.jarray(rawx)))
}
.ecor.fromB64 <- function (x) {
	rawx <- J("org.apache.commons.codec.binary.Base64")$decodeBase64(.jarray(charToRaw(x)))
	rx <- unserialize(rawx)
}



#actually create job handle and submit
initialize.HJob <- function(hconf, MAPFUN, REDUCEFUN = NULL ) {
	
	if ( class(MAPFUN) != "function" )
		stop ("mapper R function expected.")
	if ( as.character(class(hconf))!="HConf")
		stop ("hconf must be of HConf class.")
	
	hconf$set("ecor.NAMESPACES", .ecor.toB64(loadedNamespaces()))
	
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
				hconf$set("ecor.MAPFUN", basename(mapfunfilename))
				
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
				
				jconf <- hconf$as.jconf()
				
				# broadcast tempfile containing environment
				J("org.apache.hadoop.filecache.DistributedCache")$
				addCacheFile( new(J("java.net.URI"),mapfunfilename), jconf)
				
				# pre-0.23 way of doing this 
				sapply(ecor$cp[!file.info(ecor$cp)[,"isdir"]], 
						function(f)	J("org.apache.hadoop.filecache.DistributedCache")$
							addFileToClassPath(.ecor.jpath(f), jconf, .ecor.localFS()),
						simplify=T)
				
				hjob <<- new (J("org.apache.hadoop.mapreduce.Job"),jconf)
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

waitForCompletion.HJob <- function (verbose=F) {
	hjob$waitForCompletion(verbose)
}


ecor.HJob <- setRefClass("HJob",
		fields=list(hjob="S4"),
		methods=list(
				initialize=initialize.HJob,
				waitForCompletion=waitForCompletion.HJob
				)
)

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

