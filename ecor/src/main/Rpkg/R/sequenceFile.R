

.onLoad<- function () { 

ecor.Writable <- setRefClass("Writable", 
		fields=c("jwritable", "jwClass"),
		methods=list(
				initialize=initialize.Writable,
				write=function(x) NULL, 
				read=function() NULL ))

ecor.Text <- setRefClass("Text", contains="Writable", 
		methods=list(initialize=initialize.Text),
		write=write.Text,
		read=read.Text)

ecor.BytesWritable <- setRefClass("BytesWriable",contains="Writable",
		methods=list(
				initialize=initialize.BytesWritable,
				read=read.BytesWritable,
				write=write.BytesWritable
))

ecor.SequenceFileW <- setRefClass("SequenceFileW",
		fields=c("keySer","valSer","wj","isClosed"))

}


#' Constructor for the SequenceFileW class.
ecor.SequenceFileW$methods(
		initialize = initialize.SequenceFileW,
		finalize= function() {
			if ( ! isClosed ) 
				close()
		}
	)

initialize.Writable <- function (jClass ) {
	jwClass<<-jClass
	jwritable <<- new(jClass)
}

initialize.Text <- function () callSuper(J("org.apache.hadoop.io.Text"))
read.Text <- function() jwritable$toString()
write.Text <- function(x) jwritable$set(as.character(x)) 

initialize.BytesWritable <- function () callSuper(J("org.apache.hadoop.io.BytesWritable"))
read.BytesWritable <- function() jwritable$getBytes()[1:jwritable$getLength()]
write.BytesWritable <- function(x) jwritable$set(.jarray(as.raw(x)),0,length(x)-1)

	
initialize.SequenceFileW <- function(hdfsLocation,
		keyWritable =ecor.Text$new() ,
		valWritable =ecor.BytesWritable$new(),
		compressionType="BLOCK",
		jcodecClass,...) {

	callSuper(...)
	
	ct <- switch(compressionType, 
			NONE = J("org.apache.hadoop.io.SequenceFile$CompressionType")$NONE,
			BLOCK = J("org.apache.hadoop.io.SequenceFile$CompressionType")$BLOCK,
			RECORD = J("org.apache.hadoop.io.SequenceFile$CompressionType")$RECORD,
			stop (sprintf("unsupported compression type %s",ct))
	)
	
	jcodec <- if ( length(jcodecClass) == 0 ) NULL else jcodecClass$new()
	jconf <- ecor$jconf
	
	keySer <<- keySerializer
	valSer <<- valSerializer
	isClosed <<- F
	
	hdfs <- J("org.apache.hadoop.fs.FileSystem")$getFileSystem(jconf) 
	path <- new(J("org.apache.hadoop.fs.Path"),hdfsLocation)       
	jw <<- J("org.apache.hadoop.io.SequenceFile")$createWriter(
			hdfs,jconf,path,keySer$jwClass, valSer$jwClass, ct, jcodec )
	
}
	

ecor.createSeqFileWriter <- function (hdfsLocation,
		keySerializer = "Text", 
		valSerializer = "BytesWritable", 
		compressionType="BLOCK",
		codecClass = "org.apache.hadoop.io.compress.GzipCodec"
	) {
	
	w <- list()
	class(w) <- "seqfilew"
	w$keySerializer <- .ecor.serializer(keySerializer)
	w$valSerializer <- .ecor.serializer(valSerializer)
	
	jKeyClass <- ecor.writable(w$keySerializer)$getClass()$getName()
	jValClass <- ecor.writable(w$valSerializer)$getClass()$getName()
	if ( length(codecClass==1 ) )
		codec <- new(J(codecClass))
	
	ct <- switch(compressionType, 
			NONE = J("org.apache.hadoop.io.SequenceFile.CompressionType")$NONE,
			BLOCK = J("org.apache.hadoop.io.SequenceFile.CompressionType")$BLOCK,
			RECORD = J("org.apache.hadoop.io.SequenceFile.CompressionType")$RECORD,
			stop (sprintf("unsupported compression type %s",ct))
			)
	

	jconf <- ecor$jconf
	
	
	
    
} 


#' Create writable per mode strategy 
ecor.createWritable <- function(serializer, ...) UseMethod("createWritable")

#' Set writable per strategy based on R value
#' 
#' @param serializer the serializer instance 
#' @param rVal the r value to set 
ecor.toWritable <- function (serializer, rVal, ...) UseMethod("toWritable")

#' extract R value 
#' 
#' extract R value based on serialization strategy from java writable
#' 
#' @param serializer the serializer instance 
#' @param jwritable the java writable reference obtained with \link{ecor.createWritable}
ecor.fromWritable <- function(serializer, ...) UseMethod("fromWritable")

#' generic for reading writable java value
ecor.writable(serializer) <- function(serializer ) UseMethod("writable")
"ecor.writable<-"(serializer) <- function(serializer,value ) UserMethod("writable<-") 

# default serializer constructor that doesn't 
# initialize anything
.ecor.serializer <- function (serializerClass) {
	l <- list()
	class(list()) <- c("writableSerializer", serializerClass) 
} 

writable.writableSerializer <- function (serializer) {
	serializer$writable
}

"writable<-.writableSerializer" <- function (serializer, value) {
	serializer$writable <- value
}