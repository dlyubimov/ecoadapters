#
# protobuff related stuff (proto2r,r2proto)
# assumes jvm already initialized by the package. 
#

.proto.enumFromProto <- function (x) .jcall(x,"S","toString")
.proto.scalarFromProto <- function (x) .jsimplify(x)
.proto.bytesFromProto <- function (x) .jcall(x,"[B","toByteArray",evalArray=T)
.proto.doubleFromProto <- function(x) .jcall(x,"D","doubleValue")
.proto.floatFromProto <- .proto.doubleFromProto
.proto.groupFromProto <- function (x) stop ("protobuf groups are not yet supported")

.proto.msgFromProto <- function (msg, msgName, messageCatalog ) {
	if ( length(msg) ==  0 ) return(NULL)
	
	rd <- messageCatalog$rdescs[[msgName]]
	if ( length(rd)==0 ) 
		stop (sprintf ("unable to find descriptor for submessage %s.", msgName))
	
	entries <- .jcall(msg,"Ljava/util/Map;","getAllFields")
	entries <- .jcall(entries,"Ljava/util/Set;","entrySet")
	fdmap <- as.list(entries)
	
	fnames <- character(0)
	rmsg <- lapply(fdmap, function (entry) {
				fd <- .jcall(entry, "Ljava/lang/Object;", "getKey")
				fname <- .jcall(fd, "Ljava/lang/String;","getName",simplify=T)
				
				rfd <- rd[[fname]]
				
				if ( length(rfd)==0 )
					stop (sprintf( "unable to find field %s in message %s.", fname,msgName))
				
				fnames <<- c(fnames, fname)
				value <- .jcall(entry, "Ljava/lang/Object;", "getValue")
				
				if ( ! rfd$isRepeated )
					rfd$fieldFromProto(value)
				else 
					lapply(as.list(value),function(x) rfd$fieldFromProto(x))
			})
	names(rmsg) <- fnames
	rmsg
}

.proto.enumToProto <- function (x) as.character(x)
.proto.bytesToProto <- function (x) .jcall(
			"com.google.protobuf.ByteString",
			"Lcom/google/protobuf/ByteString;", 
			"copyFrom", .jarray(as.raw(x)))
.proto.int32ToProto <- function (x) .jnew ("java.lang.Integer", as.integer(x))
.proto.int64ToProto <- function (x) .jnew ("java.lang.Long",.jlong(as.numeric(x)))
.proto.boolToProto <- function (x) .jnew("java.lang.Boolean", as.logical(x))
.proto.doubleToProto <- function (x) .jnew ( "java.lang.Double", as.numeric(x))
.proto.floatToProto <- function (x) .jnew ("java.lang.Float",.jfloat(as.numeric(x)))
.proto.stringToProto <- function (x) .jnew("java.lang.String",as.character(x))
.proto.groupToProto <- function (x) stop ("protobuf groups are not yet supported")

.proto.msgToProto <- function (x, msgName, messageCatalog ) {
	
	if ( length(x) == 0 && mode(x) != "list" ) # null proposition 
		return (NULL)
	
	rd <- messageCatalog$rdescs[[msgName]]
	if ( length(rd)==0 ) 
		stop (sprintf ("unable to find descriptor for submessage %s.", msgName))
	jd <- messageCatalog$descs[[msgName]]
	
	x<- as.list(x)
	bldr <- .jcall("com.google.protobuf.DynamicMessage",
			"Lcom/google/protobuf/DynamicMessage$Builder;",
			"newBuilder", jd)
	
	lapply( names(x), function (fname ) {
				rfd <- rd[[fname]]
				if ( length (rfd)==0 ) 
					stop (sprintf ("Unable to find mapping for field %s.",fname))
				
				jfd <- rfd$jfd
				
				if ( rfd$isRepeated ) {
					sapply(x[[fname]], function(x) 
								.jcall(bldr,
										"Lcom/google/protobuf/DynamicMessage$Builder;",
										"addRepeatedField", 
										jfd, 
										.jcast(rfd$fieldToProto(x),"java.lang.Object") ))
				} else { 
					v <- rfd$fieldToProto(x[[fname]])
					if ( length(v) > 0 ) .jcall(bldr,
								"Lcom/google/protobuf/DynamicMessage$Builder;",
								"setField",
								jfd, 
								.jcast(v,"java.lang.Object"))
				}
				NA
			})
	bldr
}


initialize.DescCatalog <- function (jdesc) {
	descs <<- list()
	rdescs <<- list()
	outerMsg <<- .self$analyzeDesc(jdesc)
}

analyzeDesc.DescCatalog <- function ( jdesc ) {
	
	msgName <- .jsimplify(.jcall(jdesc,"S","getFullName"))
	
	if ( length( descs[[msgName]] )==1 ) 
		return (msgName)
	
	
	# walk the jdesc
	fields <- as.list(.jcall(jdesc,"Ljava/util/List;","getFields"))
	rfnames <- character(0)
	rfields <-
			lapply(fields,function(fd) {
						rfd <- list()
						rfd$name <- .jsimplify(.jcall(fd,"S","getName"))
						rfd$jfd <- .jcast(fd,"com.google.protobuf.Descriptors$FieldDescriptor")
						rfd$isRepeated <- .jsimplify(.jcall(fd,"Z","isRepeated"))
						protoType <- .jcall(fd,"Lcom/google/protobuf/Descriptors$FieldDescriptor$Type;","getType")
						protoType <- .jcall(protoType,"S","name")
						rfd$type <- protoType
						rfnames <<- c(rfnames,rfd$name)
						
						rfd$fieldFromProto <- switch(protoType,
								
								ENUM = .proto.enumFromProto, 
								BYTES = .proto.bytesFromProto,
								BOOL = .proto.scalarFromProto,
								DOUBLE = .proto.doubleFromProto,
								FIXED32 = .proto.scalarFromProto,
								FIXED64 = .proto.scalarFromProto, 
								FLOAT = .proto.floatFromProto,
								INT32 = .proto.scalarFromProto,
								INT64 = .proto.scalarFromProto,
								SFIXED32 = .proto.scalarFromProto,
								SFIXED64 = .proto.scalarFromProto,
								SINT32 = .proto.scalarFromProto,
								SINT64 = .proto.scalarFromProto,
								STRING = .proto.scalarFromProto,
								UINT32 = .proto.scalarFromProto,
								UINT64 = .proto.scalarFromProto,
								MESSAGE = {
									d <- .jcall(fd,"Lcom/google/protobuf/Descriptors$Descriptor;","getMessageType")
									smsgName <- analyzeDesc(d)
									function(x) .proto.msgFromProto (x,smsgName, .self) 
								},
								
								GROUP = .proto.groupFromProto,
								# UNSUPPORTED:
								stop (sprintf("unsupported conversion type %s.",protoType))
						)
						
						rfd$fieldToProto <- switch(protoType,
								
								ENUM = .proto.enumToProto,
								BYTES = .proto.bytesToProto,
								BOOL = .proto.boolToProto,
								DOUBLE = .proto.doubleToProto,
								
								FIXED32 = .proto.int32ToProto,
								FIXED64 = .proto.int64ToProto, 
								FLOAT = .proto.floatToProto,
								INT32 = .proto.int32ToProto,
								INT64 = .proto.int64ToProto,
								SFIXED32 = .proto.int32ToProto,
								SFIXED64 = .proto.int64ToProto,
								SINT32 = .proto.int32ToProto,
								SINT64 = .proto.int64ToProto,
								STRING = .proto.stringToProto,
								UINT32 = .proto.int32ToProto,
								UINT64 = .proto.int64ToProto,
								MESSAGE = {
									d <- .jcall(fd,"Lcom/google/protobuf/Descriptors$Descriptor;","getMessageType")
									smsgName <- analyzeDesc (d)
									function(x)	.jcall(
												.proto.msgToProto(x,smsgName, .self),
												"Lcom/google/protobuf/Message;",
												"build")
									
								},
								
								GROUP = .proto.groupToProto,
								# UNSUPPORTED:
								stop (sprintf("unsupported conversion type %s.",protoType))
						)
						rfd
					})
	names(rfields) <- rfnames
	
	rdescs[[msgName]] <<- rfields
	descs[[msgName]] <<- jdesc
	
	msgName
	
}

proto.DescCatalog <- setRefClass("DescCatalog",
		fields=c("descs", "rdescs", "outerMsg"),
		methods=list(
				initialize = initialize.DescCatalog,
				analyzeDesc = analyzeDesc.DescCatalog
		))

# map all messages in descriptor into R5 class
# 
# load, parse descriptor information into R5 catalog 
# object (DescCatalog class)
# 
# @param descriptorUrl descriptor URL specified same 
# way as in other types of adapters: either hdfs url 
# pointing to proto descriptor file with outer message specification
# or the generated message classname itself
# 
proto.desc <- function (descriptorUrl ) { 
	u <- as.character(descriptorUrl)
	
	jdesc <- if ( length( grep("^hdfs://",u) ) > 0 ) 
				.jcall("com/inadco/ecoadapters/EcoUtil", 
						"Lcom/google/protobuf/Descriptors$Descriptor;", 
						"inferDescriptorFromFilesystem",u)
			else  
				.jcall("com/inadco/ecoadapters/EcoUtil", 
						"Lcom/google/protobuf/Descriptors$Descriptor;", 
						"inferDescriptorFromClassName",u)
	
	## pull the information from descriptor into R structures 
	## in order to avoid making java calls later
	proto.DescCatalog$new(jdesc)
}

proto.fromProtoRaw <- function (x, descCatalog) { 
	msg <- .jcall("com.google.protobuf.DynamicMessage",
			"Lcom/google/protobuf/DynamicMessage;",
			"parseFrom",
			descCatalog$descs[[descCatalog$outerMsg ]],
			as.raw(x))
	.proto.msgFromProto(msg, descCatalog$outerMsg, descCatalog )
}
proto.fromProtoMsg <- function (x, descCatalog) .proto.msgFromProto(x, descCatalog$outerMsg, descCatalog )
proto.toProtoBldr <- function (x, descCatalog) .proto.msgToProto(x,descCatalog$outerMsg, descCatalog)
proto.toProtoMsg <- function (x, descCatalog) .jcall(proto.toProtoBldr(x,descCatalog),"Lcom/google/protobuf/DynamicMessage;","build")
proto.toProtoRaw <- function (x, descCatalog) .jcall(proto.toProtoMsg(x,descCatalog),"[B","toByteArray",evalArray=T)

