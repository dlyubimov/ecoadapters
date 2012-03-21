

setJClass.Writable <- function (jClass ) {
	jwClass<<-jClass
	jwritable <<- new(jClass)
}


initialize.Text <- function() setJClass(J("org.apache.hadoop.io.Text"))
read.Text <- function() jwritable$toString()
write.Text <- function(x) {
	x <- as.character(x)
	if ( length(x)!= 1 ) stop ("Writable: only writing singletons is allowed.")
	
	# this is appallingly slow!
	# jwritable$set(x)
	.jcall(jwritable, "V", "set", x)
}

initialize.BytesWritable <- function () setJClass(J("org.apache.hadoop.io.BytesWritable"))
read.BytesWritable <- function() jwritable$getBytes()[1:jwritable$getLength()]
write.BytesWritable <- function(x) {
	len <- length(x)
	x <- if (len == 0  ) .jnull("[B") else .jarray(as.raw(x)) 
	
	#jwritable$set(.jarray(as.raw(x)),0,length(x)-1)
	.jcall(jwritable,"V","set", x, 0, len-1 )
}

initialize.ProtoWritable <- function ( protoDesc ) { 
	setJClass(J("org.apache.hadoop.io.BytesWritable"))
	protodesc <<- proto.desc(protoDesc)
}

read.ProtoWritable <- function() 
	proto.fromProtoRaw(jwritable$getBytes()[1:jwritable$getLength()], protodesc)

write.ProtoWritable <- function ( rlist ) { 
	x <- proto.toProtoRaw(rlist,protodesc)
	len <- length(x)
	x <- if (len == 0  ) .jnull("[B") else .jarray(as.raw(x)) 
	
	.jcall(jwritable,"V","set", x, 0L, len - 1L)
}



#' Constructor for the SequenceFileW class
#' 
#' Initialize a new instance 
#' 
#' TODO: codecs requiring native libraries are not loaded. 
#' Need yet to figure how to solve that with rJava etc.
#' 
#' @param hdfsLocation location of the sequence file being written. 
#' @param keyWritable R5 instance of a class derived from Writable class 
#' to establish serialization for the key
#' @param valWritable R5 instance of a class derived from Writable class
#' @param compressionType For possible (character) values see 
#' \code{org.apache.hadoop.io.SequenceFile$CompressionType} enumeration.
#' Currently, possible values include 'NONE', 'BLOCK' or 'RECORD'.
#' @param jcodecClass java codec to use. Should be result of 
#' J("java-codec-class-name").
initialize.SequenceFileW <- function(hdfsLocation,
		keyWritable =ecor.Text$new() ,
		valWritable =ecor.Text$new(),
		compressionType="BLOCK",
		jcodecClass = J("org.apache.hadoop.io.compress.DefaultCodec"), ...) {
	
	callSuper(...)
	
	ct <- switch(compressionType, 
			NONE = J("org.apache.hadoop.io.SequenceFile$CompressionType")$NONE,
			BLOCK = J("org.apache.hadoop.io.SequenceFile$CompressionType")$BLOCK,
			RECORD = J("org.apache.hadoop.io.SequenceFile$CompressionType")$RECORD,
			stop (sprintf("unsupported compression type %s",ct))
	)
	
	jcodec <- if ( length(jcodecClass) == 0 ) NULL else new(jcodecClass)
	jconf <- ecor$jconf
	
	if ( ! is(keyWritable, "Writable") )
		stop("key writable must be of Writable class.")
	if ( ! is(valWritable, "Writable") )
		stop("value writable must be of Writable class.")
	
	keyw <<- keyWritable
	valw <<- valWritable
	isClosed <<- F
	
	hdfs <- J("org.apache.hadoop.fs.FileSystem")$get(jconf) 
	path <- new(J("org.apache.hadoop.fs.Path"),hdfsLocation)       
	jw <<- J("org.apache.hadoop.io.SequenceFile")$createWriter(
			hdfs,jconf,path,keyw$jwClass$class, valw$jwClass$class, ct, jcodec )
	
}

append.SequenceFileW <- function (key,value) {
	
	if (isClosed) stop ("attempt to write to a closed writer.")

	kw <- .jcast(keyw$jwritable, "org.apache.hadoop.io.Writable")
	vw <- .jcast(valw$jwritable, "org.apache.hadoop.io.Writable")
	
	# use "vectorized" version if arrays supplied.
#	if ( length(key)!= 1 || length(value) != 1 ) {
#		df <- data.frame(key=key, value=value, stringsAsFactors=F)
#		sapply(1:nrow(df), function(x) {
#
#					# this is still pretty slow. 
#					# but more tolerable at this point. 
#					# we may have to wrap it all in a single java-side 
#					# helper call later.
#					
#					
#					keyw$write(df[x,"key"])
#					valw$write(df[x,"value"])
#					
#					#slow!
#					#jw$append(keyw$jwritable, valw$jwritable )
#					
#					.jcall(jw,"V","append", kw,vw)
#				})
#		NULL
#	} else { 
		keyw$write(key)
		valw$write(value)
		
		# slow!!!
		# jw$append(keyw$jwritable, valw$jwritable )
		.jcall(jw,"V","append",kw,vw)
#	}
}

close.SequenceFileW <- function()  
  	if( !isClosed ) { 
		isClosed <<- T; jw$close() 
	}

###############
# R5 class declarations
###############

ecor.Writable <- setRefClass("Writable", 
		fields=c("jwritable", "jwClass" ),
		methods=list(
				setJClass=setJClass.Writable,
				write=function(x) NULL, 
				read=function() NULL 
		))

ecor.Text <- setRefClass("Text", 
		contains="Writable",
		methods=list(
				initialize=initialize.Text,
				write=write.Text,
				read=read.Text
		))

ecor.BytesWritable <- setRefClass("BytesWritable",contains="Writable",
		methods=list(
				initialize=initialize.BytesWritable,
				read=read.BytesWritable,
				write=write.BytesWritable
		))

ecor.ProtoWritable <- setRefClass("ProtoWritable", contains="Writable",
		fields = c("protodesc"),
		methods = list (
				initialize=initialize.ProtoWritable,
				read=read.ProtoWritable,
				write=write.ProtoWritable
				))

ecor.SequenceFileW <- setRefClass("SequenceFileW",
		fields=c("keyw","valw","jw","isClosed"))

ecor.SequenceFileW$methods(
		initialize = initialize.SequenceFileW,
		append = append.SequenceFileW,
		close = close.SequenceFileW,
		finalize= function() close()
		
)

