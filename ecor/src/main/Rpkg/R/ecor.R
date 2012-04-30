#' protobuf-R-MR adapters.
#' 
#' @description
#' 
#' Run R with mapReduce on (mostly) protobuf input as part of ecoadapters package.
#'  
#' @details 
#' to enable running R on task nodes:
#' 
#' \enumerate{
#' \item{ install R } 
#' \item{ \code{install.packages("rJava")}}
#' \item{ configure R_HOME in the backend ( on Ubuntu it is usually /usr/lib/R)}
#' \item{ add whatever path is returned by \code{system.file("jri",package="rJava")}
#'   to the \code{-Djava.library.path=...} setting in the backend
#' }
#' }
#' 
#' example of data node properties added to core-site.xml:
#'  
#' \preformatted{
#' <property>
#'   <name>mapred.child.env</name>
#'   <value>R_HOME=/usr/lib/R</value>
#' </property>
#'
#' <property>
#'   <name>mapred.map.child.java.opts</name>
#'   <value>-Djava.library.path=/home/dmitriy/R/x86_64-pc-linux-gnu-library/2.14/rJava/jri</value>
#' </property>
#' }
#' 
#' I also found that i may need to use <final> spec with some of those in the data nodes to lock them 
#' from overrides.\cr\cr
#' 
#' Alternative (and perhaps better) way is to define those properties for tasktracker 
#' in hadoop-env.sh. At this time I couldn't make it happen. \cr\cr
#' 
#' \strong{Tip:} The following command should produce location dir of libjri.so:\cr\cr
#' 
#' \code{R --vanilla <<< 'system.file("jri", package="rJava")'}\cr\cr
#' 
#' 
#' @docType package
#' @name ecor
#' @exportPattern "^ecor\\.|^proto\\.|^hdfs\\."
#' @import rJava
#' @include protobuf.R
#' @include hdfs.R
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
		options(error=quote(dump.frames("errframes", F)))
		
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
	# Besides, having to make java calls on Job is a tight integration, too.
	
	consts["INPUT_FORMAT"] <- "mapreduce.inputformat.class"
	consts["OUTPUT_FORMAT"] <- "mapreduce.outputformat.class"
	consts["MAP"] <- "mapreduce.map.class"
	consts["COMBINE" ] <- "mapreduce.combine.class"
	consts["REDUCE" ] <- "mapreduce.reduce.class"
	consts["PARTITION"] <- "mapreduce.partitioner.class"
	consts["NAME"] <- "mapred.job.name"
	consts["INPUT"] <- "mapred.input.dir"
	consts["OUTPUT"] <- "mapred.output.dir"
	consts["MAPOUTPUTKEY_CLASS"] <- "mapred.mapoutput.key.class"
    consts["MAPOUTPUTVALUE_CLASS"] <- "mapred.mapoutput.value.class"
    consts["OUTPUTKEY_CLASS"] <- "mapred.output.key.class"
	consts["OUTPUTVALUE_CLASS"] <- "mapred.output.value.class"
	consts["REDUCE_TASKS"] <- "mapred.reduce.tasks"
	
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
	# TODO: use hadoop classpath to establish classpath instead.
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
	
	# this doesn't quite work yet. switch back to HADOOP_HOME
#	hcp <- strsplit(system("hadoop classpath",intern=T),":|;")
#	if ( length(hcp) == 0 )
#		stop ("Can't execute \"hadoop classpath\" successfully.");
#	hcp
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
	if ( ! file.exists(hbconf) )
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

#################################################
# MR stuff starts here.                         # 
#################################################


#' @title 
#' hadoop MR
#' 
#' @description 
#' simple MR wrapper for most typical single-input MR scenario.
#' 
#' @param input hdfs file path(s) or glob(s) for MR input
#' @param output hdfs output path (must not exist unless \code{overwrite==T})
#' @param MAPFUN map function in a form MAPFUN(key,value)
#' @param REDUCEFUN reduce function in a form (key2, valuelist)
#' @param MAPSETUPFUN map setup function (no parameters passed in)
#' @param REDUCESETUPFUN reduce task setup function (no parameters passed in)
#' @param reduceTasks number of reduce tasks to run
#' @param overwrite if TRUE and \code{output} directory already exists, it will be deleted first.
#' @param wait Wait till job is complete. One can start multiple jobs and wait for them later.
#' @return if \code{wait==FALSE} returns ecor.HJob R5 object representing job handle . Otherwise, 
#' if waiting for job completion is requested, returns result of \code{HJob$waitForCompletion()}
#' which is TRUE if job was successful and FALSE otherwise.
#' 
#' @details 
#' launches simple one-input hadoop mapReduce job with the functions specified.
#' The input in this version is assumed to be Hadoop sequence file. Supported key types 
#' or values at this point are IntWritable, LongWritable, DoubleWritable, Text or BytesWritable.
#' They are transformed to simple R types per rJava simple type convertion. 
#' BytesWritable is converted to raw.
#' 
#' @section 
#' map or reduce task may collect outputs using \link{ecor.collect} method.
ecor.MR <- function(input, output, MAPFUN, REDUCEFUN = NULL, MAPSETUPFUN = NULL, REDUCESETUPFUN = NULL, 
		reduceTasks= if(length(REDUCEFUN)==0) 0 else 1, overwrite=F, wait=T) {
	if ( length (input)>1 )
		input <- paste(input, collapse = ";")
	
	hconf <- ecor.HConf$new()
	hconf$setInput(input)
	hconf$setOutput(output)
	hconf$setMapper(MAPFUN)
	
	if ( length(MAPSETUPFUN)==1)
		hconf$setMapSetup(MAPSETUPFUN)
	if ( length(REDUCEFUN)==1)
		hconf$setReducer(REDUCEFUN)
	if ( length(REDUCESETUPFUN)==1)
		hconf$setReduceSetup(REDUCESETUPFUN)
	hconf$setReduceTasks(reduceTasks)
	
	hjob <- hconf$mrSubmit(overwrite)
	
	if (wait) 
		hjob$waitForCompletion()
	else
		hjob
} 

#' initialize new HConf instance.
#' 
#' @method initialize HConf
#' @param jconf rJava reference to \code{o.a.h.conf.Configuration}
initialize.HConf <- function (jconf=NULL) {
  
  props <<- character(0)
  
  if ( length(jconf)>0) {
    iter <- jconf$iterator()
    while (iter$hasNext() ) { 
      map.entry <- iter$`next`()
      props[as.character(map.entry$getKey())] <<- as.character(map.entry$getValue())
    }
  }
  props[ecor$consts["MAP"]] <<- "com.inadco.ecoadapters.r.RMapper"
  props[ecor$consts["REDUCE"]] <<- "com.inadco.ecoadapters.r.RReducer"
  props[ecor$consts["INPUT_FORMAT"]] <<- "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat"
  props[ecor$consts["OUTPUT_FORMAT"]] <<- "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat"
  
  props[ecor$consts["MAPOUTPUTKEY_CLASS"]] <<- "org.apache.hadoop.io.Text"
  props[ecor$consts["MAPOUTPUTVALUE_CLASS"]] <<- "org.apache.hadoop.io.BytesWritable"

  props[ecor$consts["OUTPUTKEY_CLASS"]] <<- "org.apache.hadoop.io.Text"
  props[ecor$consts["OUTPUTVALUE_CLASS"]] <<- "org.apache.hadoop.io.BytesWritable"
  
  hname <<- "R-Job"
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
setMapper.HConf <- function(value) mapfun <<- value
getMapper.HConf <- function () mapfun
setReducer.HConf <- function (value) reducefun <<- value
getReducer.HConf <- function () reducefun


setInput.HConf <- function (value) props[ecor$consts["INPUT"]] <<- value
getInput.HConf <- function () props[ecor$consts["INPUT"]]
setOutput.HConf <- function(value) props[ecor$consts["OUTPUT"]] <<- value
getOutput.HConf <- function () props[ecor$consts["OUTPUT"]]

setName.HConf <- function(value) hname <<- value

mrSubmit.HConf <- function (overwrite = F) ecor.HJob$new(.self, overwrite)

setMapSetup.HConf <- function (value) mapsetupfun<<- value
setReduceSetup.HConf <- function (value) reducesetupfun <<- value
setReduceTasks.HConf <- function (value) props[ecor$consts["REDUCE_TASKS"]] <<- as.character( value) 

#' R5 class holding MR configuration etc. stuff.
#' 
#' 
ecor.HConf <- setRefClass("HConf", 
		fields=list(props="character",
		mapsetupfun = "function",
        mapfun="function",
		reducesetupfun="function",
		reducefun="function",
		namespaces="character",
        hname="character"),
		methods=list(
				initialize =initialize.HConf,
				as.jconf =as.jconf.HConf,
				set = set.HConf,
				get = get.HConf,
				setInputFormat = setInputFormat.HConf,
				getInputFormat = getInputFormat.HConf,
				setOutputFormat = setOutputFormat.HConf,
				getOutputFormat = getOutputFormat.HConf,
				setMapSetup = setMapSetup.HConf,
				setReduceSetup = setReduceSetup.HConf,
				setMapper = setMapper.HConf,
				getMapper = getMapper.HConf,
				setReducer = setReducer.HConf,
				getReducer = getReducer.HConf,
				setInput = setInput.HConf,
				getInput = getInput.HConf,
				setReduceTasks = setReduceTasks.HConf,
        setOutput = setOutput.HConf,
        getOutput = getOutput.HConf,
        setName = setName.HConf,
				mrSubmit = mrSubmit.HConf
				))
		
##################################
# generic MR job driver          #
##################################


.ecor.toB64 <- function(x) {
	rawx <- serialize(x, NULL, ascii = F)
	rawToChar(J("org.apache.commons.codec.binary.Base64")$encodeBase64(.jarray(rawx)))
}
.ecor.fromB64 <- function (x) {
	rawx <- J("org.apache.commons.codec.binary.Base64")$decodeBase64(.jarray(charToRaw(x)))
	rx <- unserialize(rawx)
}



#actually create job handle and submit
initialize.HJob <- function(hconf, overwrite=F ) {
  
  if ( length(hconf$mapfun)==0 )
    stop ("Mapper not specified in configuration.")
  
  jfs <- hdfs.dfs()
  
  #set up job temporary dir 
  tstamp<- Sys.time()
  tstamp <- format(tstamp,"%Y%m%d_%H%M%S")
  r <- sprintf("%08X",as.integer(runif(1)*(2^32-1)-2^31))
  tstamp <- sprintf("%s_%s",tstamp,r)
  
  tempDir <<- file.path("/temp","R",tstamp)
  
  hconf$namespaces <- loadedNamespaces()
  
  fcleanup <- character(0)
  
  rjobfile <- tempfile()
  fcleanup<- c(fcleanup, rjobfile)
  
  f <- file(rjobfile, open="wb")
  tryCatch({
        # my tests seem to indicate 
        # that this serializes all the function 
        # environment too.
        serialize(hconf, f, ascii = F)
      },
      finally = close(f)
  )
  
  hconf$set("ecor.hconffile", basename(rjobfile))
  
  # map-only?
  if ( length(hconf$reducefun)==0)
	  setReduceTasks(0L)
  
  #sorry, we have to hijack mapred.child.java.opts here.
  #  lp <- paste(hconf$javalibpath,collapse = ":")
  #  hconf$set("mapred.child.java.opts",
  #      sprintf("%s -Djava.library.path=%s", hconf$memopts, lp))

  jconf <- hconf$as.jconf()

  # not clear if it does any good
  # explicitly unset any jvm options because it would mask 
  # the ones set in the backend by default:
  jconf$set("mapred.child.java.opts", "" )
  
  
  # broadcast tempfile containing environment
  J("com.inadco.ecoadapters.r.RMRHelper")$addFileToCache(jconf, .jarray(rjobfile), tempDir, F);
  
  jricp <- list.files(system.file("jri",package="rJava"),
      full.names=T, pattern ="\\.jar$")
  
  cp <- c(ecor$cp,jricp)

  J("com.inadco.ecoadapters.r.RMRHelper")$addFileToCache(jconf, .jarray(cp), tempDir, T);
  
  # pre-0.23 way of doing this 
#  sapply(cp[!file.info(cp)[,"isdir"]], 
#      function(f)	J("org.apache.hadoop.filecache.DistributedCache")$
#        addFileToClassPath(hdfs.path(f), jconf, hdfs.localfs()),
#      simplify=T)
  
  hjob <<- new (J("org.apache.hadoop.mapreduce.Job"),jconf)
  
  if ( overwrite )  
    hdfs.delete(jfs, hconf$getOutput(), T)
  
  hjob$setJobName(hconf$hname)
  hjob$submit() 

  file.remove(fcleanup[file.exists(fcleanup)])
}

waitForCompletion.HJob <- function (verbose=F) {
	tryCatch( 
			hjob$waitForCompletion(verbose),
			finally = if ( length(tempDir)==1 ) 
				hdfs.delete(hdfs.dfs(), tempDir, T)
	)
}


ecor.HJob <- setRefClass("HJob",
		fields=list(
				hjob="jobjRef",
				tempDir="character"),
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

#' 
#' @param e the simple error object
.ecor.stackTrace <- function (e) {
	ef <- getAnywhere("errframes")
	ef <- ef$objs[which(ef$where==".GlobalEnv")]
	
	if ( length(ef)==1) {
		paste( c(as.character(e), 
					"frame stack: ",
					names(ef)),
			collapse = "\n") 
	} else {
		as.character(e)
	}
}

.ecor.tasksetup <- function ( jconf, hconffile, mapsetup=T ) {
	
	tryCatch({
				hconf <- NULL 
				
				f <- file(hconffile, "rb")
				tryCatch({
							hconf <- unserialize(f)
						},
						finally = {
							close(f)
						})
				
				# frontend packages translated to backend 
				# to load here as well.
				packages <- hconf$namespaces
				
				if ( length(packages) == 0 ) 
					stop ("no packages in the job configuration")
				
				require(packages)
				
				ecor$hconf <<- hconf
				ecor$jconf <<- jconf
				
				if ( mapsetup ) { 
					if (length(ecor$hconf$mapsetupfun) ==1)
						ecor$hconf$mapsetupfun();
				} else {
					if (length(ecor$hconf$reducesetupfun) ==1)
						ecor$hconf$reducesetupfun();
					
				}
				
				
				T
			},
			error = function(e) .ecor.stackTrace(e)
	)
}

.ecor.maptask <- function (jconf, jkey, jvalue ) {
	tryCatch({
				ecor$hconf$mapfun(jkey,jvalue)
				T
			}, 
			error=function(e) .ecor.stackTrace(e))
}


.ecor.reducetask <- function ( jconf, key, jvalueIter ) {
	tryCatch({
				if ( length(ecor$hconf$reducefun)==0) 
					stop("R reducer not configured. Perhaps you wanted to do map-only job?")
				
				vals <- list()
				len <- 0L
				
				# this will not work well with skewed data though.  
				
				while (.jcall(jvalueIter,"Z","hasNext")) {
					bw <- .jcall(jvalueIter,"Ljava/lang/Object;","next")
					v <- .jcall(bw,"[B","getBytes",evalArray=T)
					l <- .jcall(bw,"I", "getLength",simplify=T)
					len <- len +1L 
					vals [[len]] <- unserialize(v[1:l])
				}
				
				ecor$hconf$reducefun(key, vals)
				T
			}, 
			error=function(e) .ecor.stackTrace(e)
	)
}

#' @title 
#' Collect key and values from R tasks
#' 
#' @description 
#' Collect keys and values from R tasks. 
#' 
#' @details 
#' Currently, key is coerced with as.character() and 
#' saved as Text writable. Value is R-serialized and saved 
#' as BytesWritable.
#' 
#' @param key key (will be coerced to a character vector of length 1)
#' @param value R object to be serialized 
ecor.collect <- function (key, value) {

	jkey <- as.character(key)
	if ( length(jkey)!= 1)
		stop ("must be exactly one character key value")
	
	jvalue <- .jarray(serialize(value,NULL))
	
	.jcall(ecocollector____,"V","add",jkey,jvalue)
	
}

