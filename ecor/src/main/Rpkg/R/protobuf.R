#
# protobuff related stuff (proto2r,r2proto)
# assumes jvm already initialized by the package. 
#

proto.desc <- function (descriptorUrl ) { 
	u <- as.character(descriptorUrl)
	
	if ( length( grep("^hdfs://",u) ) > 0 ) 
		.jcall("com/inadco/ecoadapters/EcoUtil", 
				"Lcom/google/protobuf/Descriptors$Descriptor;", 
				"inferDescriptorFromFilesystem",u)
	else  
		.jcall("com/inadco/ecoadapters/EcoUtil", 
				"Lcom/google/protobuf/Descriptors$Descriptor;", 
				"inferDescriptorFromClassName",u)
}


proto.fromProtoMsg <- function ( message ) {
	
	if ( length(message) ==  0 ) return(NULL)
	
	entries <- .jcall(message,"Ljava/util/Map;","getAllFields")
	entries <- .jcall(entries,"Ljava/util/Set;","entrySet")
	fdmap <- as.list(entries)
	
	fnames <- NULL
	vals <- lapply(fdmap,function(entry) {
				fd <- .jcall(entry, "Ljava/lang/Object;", "getKey")
				value <- .jcall(entry, "Ljava/lang/Object;", "getValue")
				fnames <- c(fnames, .jcall(fd, "Ljava/lang/String;","getName",simplify=T))
				
				v<- NULL
				if ( .jcall(fd,"Z","isRepeated" ) ) 
					.proto.repeatedFieldFromProto(value,fd)
				else
					.proto.fieldFromProto(value,fd)
			})
	
	names(vals) <- fnames
	
	vals
}

proto.fromProtoRaw <- function ( rawmsg, descriptor ) 
	proto.fromProtoMsg(.jcall(
					"com.google.protobuf.DynamicMessage", 
					"Lcom/google/protobuf/DynamicMessage;",
					"parseFrom",
					descriptor, 
					.jarray(as.raw(rawmsg))))

proto.toProtoBldr <- function ( x, descriptor ) {
	
	if ( length(x) == 0 && mode(x) != "list" ) # null proposition 
		return (NULL)
	
	x<- as.list(x)
	bldr <- .jcall("com.google.protobuf.DynamicMessage",
			"Lcom/google/protobuf/DynamicMessage$Builder;",
			"newBuilder", descriptor)
	
	lapply( names(x), function (fname ) {
				fd <- .jcall(descriptor,
						"Lcom/google/protobuf/Descriptors$FieldDescriptor;",
						"findFieldByName",fname)
				if ( length(fd) == 0 ) 
					stop (sprintf("Unable to find field descriptor for '%s'.",fname))
				else {
					if ( .jcall(fd,"Z","isRepeated") ) {
						v <- .proto.repeatedFieldToProto(x[[fname]],fd)
						lapply(v, function(v) if ( length(v) >0 ) 
										.jcall(bldr,
												"Lcom/google/protobuf/DynamicMessage$Builder;",
												"addRepeatedField", 
												fd, 
												.jcast(v,"java.lang.Object") ))
					} else { 
						v <- .proto.fieldToProto(x[[fname]],fd)
						if ( length(v) > 0 ) .jcall(bldr,
									"Lcom/google/protobuf/DynamicMessage$Builder;",
									"setField",
									fd, 
									.jcast(v,"java.lang.Object"))
					}
				} 
			})
	bldr
}

proto.toProtoMsg <- function (x, descriptor ) .jcall(
			proto.toProtoBldr(x, descriptor),
			"Lcom/google/protobuf/DynamicMessage;","build")
proto.toProtoRaw <- function (x, descriptor ) .jcall(
			proto.toProtoMsg(x, descriptor),
			"[B", "toByteArray",evalArray=T)

.proto.fieldToProto <- function (x, fd ) {
	
	# cat ( fd$getType()$name() )
	if ( length(x)==0 && mode(x) != "list" ) 
		# null proposition
		# unless it is an empty list 
		# which may be transformed to an empty builder.
		return (NULL)
	
	# slow
	# protoType <- fd$getType()$name()
	protoType <- .jcall(fd,"Lcom/google/protobuf/Descriptors$FieldDescriptor$Type;","getType")
	protoType <- .jcall(protoType,"S","name")
	
	switch(protoType,
			
			ENUM = as.character(x), # ?
			
			BYTES = .jcall("com.google.protobuf.ByteString",
					"Lcom/google/protobuf/ByteString;", 
					"copyFrom", .jarray(as.raw(x))),
			
			BOOL = .jnew("java.lang.Boolean", as.logical(x)),
			
			DOUBLE = .jnew ( "java.lang.Double", as.numeric(x)),
			
			FIXED32 = .jnew ("java.lang.Integer", as.integer(x)),
			FIXED64 = .jnew ("java.lang.Long",.jlong(as.numeric(x))), 
			FLOAT = .jnew ("java.lang.Float",.jfloat(as.numeric(x))),
			INT32 = .jnew ("java.lang.Integer", as.integer(x)),
			INT64 = .jnew ("java.lang.Long",.jlong(as.numeric(x))),
			SFIXED32 = .jnew ("java.lang.Integer", as.integer(x)),
			SFIXED64 = .jnew ("java.lang.Long",.jlong(as.numeric(x))),
			SINT32 = .jnew ("java.lang.Integer", as.integer(x)),
			SINT64 = .jnew ("java.lang.Long",.jlong(as.numeric(x))),
			STRING = .jnew("java.lang.String",as.character(x)),
			UINT32 = .jnew ("java.lang.Integer", as.integer(x)),
			UINT64 = .jnew ("java.lang.Long",.jlong(as.numeric(x))),
			MESSAGE = {
				d <- fd$getMessageType()
				proto.toProtoMsg (x, d)
			},
			
			# UNSUPPORTED:
			# GROUP = ,	
			stop (sprintf("unsupported conversion type %s.",protoType))
	)
}

.proto.repeatedFieldToProto <- function (x, fd) sapply(x, function(x) .proto.fieldToProto(x,fd))

.proto.fieldFromProto <- function(x, fd ) { 
	
	# shouldn't be any NULLs when converting from proto
	# protoType <- fd$getType()$name()
	# these descriptors probably should be cached in the R structures 
	# to speed things up even more, later.
	
	protoType <- .jcall(fd,"Lcom/google/protobuf/Descriptors$FieldDescriptor$Type;","getType")
	protoType <- .jcall(protoType,"S","name")
	
	switch(protoType,
			
			ENUM = .jcall(x,"S","toString"), 
			BYTES = .jcall(x,"[B","toByteArray",evalArray=T),
			BOOL = .jsimplify(x),
			DOUBLE = .jcall(x,"D","doubleValue"),
			FIXED32 = .jsimplify(x),
			FIXED64 = .jsimplify(x), 
			FLOAT = .jcall(x,"D","doubleValue"), #?
			INT32 = .jsimplify(x),
			INT64 = .jsimplify(x),
			SFIXED32 = .jsimplify(x),
			SFIXED64 = .jsimplify(x),
			SINT32 = .jsimplify(x),
			SINT64 = .jsimplify(x),
			STRING = .jsimplify(x),
			UINT32 = .jsimplify(x),
			UINT64 = .jsimplify(x),
			MESSAGE = proto.fromProtoMsg (x),
			
			# UNSUPPORTED:
#			GROUP = ,	
			stop (sprintf("unsupported conversion type %s.",protoType))
	)
}

.proto.repeatedFieldFromProto <- function (protolist,fd) 
	lapply(as.list(protolist),function(protoval) .proto.fieldFromProto(protoval,fd))


