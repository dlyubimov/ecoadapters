#
# protobuff related stuff (proto2r,r2proto)
# assumes jvm already initialized by the package. 
#

proto.desc <- function (descriptorUrl ) { 
	u <- as.character(descriptorUrl)
	
	if ( length( grep("^hdfs://",u) ) > 0 ) 
		J("com.inadco.ecoadapters.EcoUtil")$inferDescriptorFromFilesystem(u)
	else  
		J("com.inadco.ecoadapters.EcoUtil")$inferDescriptorFromClassName(u)
}


proto.fromProtoMsg <- function ( message ) {

	if ( length(message) ==  0 ) return(NULL)
	
	
	fdmap <- as.list(message$getAllFields()$entrySet())
	
	vals <- lapply(fdmap,function(entry) {
		fd <- entry$getKey()
		value <- entry$getValue()
		
		v<- NULL
		if ( fd$isRepeated() ) 
			.proto.repeatedFieldFromProto(value,fd)
		else
			.proto.fieldFromProto(value,fd)
	})
	names(vals) <- sapply(fdmap, function(entry) entry$getKey()$getName() ) 
	vals
}

proto.fromProtoRaw <- function ( rawmsg, descriptor ) 
		proto.fromProtoMsg(J("com.google.protobuf.DynamicMessage")$parseFrom(descriptor, .jarray(as.raw(rawmsg))))

proto.toProtoBldr <- function ( x, descriptor ) {
	
	if ( length(x) == 0 && mode(x) != "list" ) # null proposition 
		return (NULL)
	
	x<- as.list(x)
	bldr <- J("com.google.protobuf.DynamicMessage")$newBuilder(descriptor)
	
	lapply( names(x), function (fname ) {
				fd <- descriptor$findFieldByName(fname)
				if ( length(fd) == 0 ) 
					stop (sprintf("Unable to find field descriptor for '%s'.",fname))
				else {
					if ( fd$isRepeated() ) {
						v <- .proto.repeatedFieldToProto(x[[fname]],fd)
						lapply(v, function(v) if ( length(v) >0 ) bldr$addRepeatedField(fd, v) )
					} else { 
						v <- .proto.fieldToProto(x[[fname]],fd)
						if ( length(v) >0 ) bldr$setField(fd, v)
					}
				} 
			})
	bldr
}

proto.toProtoMsg <- function (x, descriptor ) proto.toProtoBldr(x, descriptor)$build()
proto.toProtoRaw <- function (x, descriptor ) proto.toProtoMsg(x, descriptor)$toByteArray()

.proto.fieldToProto <- function (x, fd ) {
	
	# cat ( fd$getType()$name() )
	if ( length(x)==0 && mode(x) != "list" ) 
		# null proposition
		# unless it is an empty list 
		# which may be transformed to an empty builder.
		return (NULL)
	
	protoType <- fd$getType()$name()
	switch(protoType,
			
			ENUM = as.character(x), # ?
			BYTES = J("com.google.protobuf.ByteString")$copyFrom(.jarray(as.raw(x))),
			BOOL = new(J("java.lang.Boolean"), as.logical(x)),
			DOUBLE = new ( J("java.lang.Double"), as.numeric(x)),
			FIXED32 = new (J("java.lang.Integer"), as.integer(x)),
			FIXED64 = new(J("java.lang.Long"),.jlong(as.numeric(x))), 
			FLOAT = new (J("java.lang.float"),.jfloat(as.numeric(x))),
			INT32 = new (J("java.lang.Integer"), as.integer(x)),
			INT64 = new(J("java.lang.Long"),.jlong(as.numeric(x))),
			SFIXED32 = new (J("java.lang.Integer"), as.integer(x)),
			SFIXED64 = new(J("java.lang.Long"),.jlong(as.numeric(x))),
			SINT32 = new (J("java.lang.Integer"), as.integer(x)),
			SINT64 = new(J("java.lang.Long"),.jlong(as.numeric(x))),
			STRING = as.character(x),
			UINT32 = new (J("java.lang.Integer"), as.integer(x)),
			UINT64 = new(J("java.lang.Long"),.jlong(as.numeric(x))),
			MESSAGE = {
				d <- fd$getMessageType()
				proto.toProtoMsg (x, d)
			},
			
			# UNSUPPORTED:
#			GROUP = ,	
			stop (sprintf("unsupported conversion type %s.",protoType))
	)
}

.proto.repeatedFieldToProto <- function (x, fd) sapply(x, function(x) .proto.field2Proto(x,fd))

.proto.fieldFromProto <- function(x, fd ) { 
	# shouldn't be any NULLs when converting from proto
	protoType <- fd$getType()$name()
	switch(protoType,

			ENUM = x$toString(), # ?
			BYTES = as.raw(x$toByteArray()),
			BOOL = as.logical(x),
			DOUBLE = as.numeric(x),
			FIXED32 = as.integer(x),
			FIXED64 = as.numeric(x), 
			FLOAT = as.numeric(x),
			INT32 = as.integer(x),
			INT64 = as.numeric(x),
			SFIXED32 = as.integer(x),
			SFIXED64 = as.numeric(x),
			SINT32 = as.integer(x),
			SINT64 = as.numeric(x),
			STRING = as.character(x),
			UINT32 = as.integer(x),
			UINT64 = as.numeric(x),
			MESSAGE = proto.fromProtoMsg (x),
			
			# UNSUPPORTED:
#			GROUP = ,	
			stop (sprintf("unsupported conversion type %s.",protoType))
	)
}

.proto.repeatedFieldFromProto <- function (protolist,fd) 
	lapply(as.list(protolist),function(protoval) .proto.fieldFromProto(protoval,fd))


