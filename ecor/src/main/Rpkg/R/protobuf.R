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

.proto.parseNonrepeatedField <- function (x, fd ) {
	
	# cat ( fd$getType()$name() )
	switch(fd$getType()$name(),
			BYTES = { 
				as.raw(x)
			},
			{
				stop (sprintf("unsupported conversion type %s.",fd$getType()$name()))
			})
	
	
}

.proto.parseRepeatedField <- function (x, fd ) sapply(x, function(x) .proto.parseNonrepeatedField(x,fd))


proto.toR <- function ( message, descriptor ) {
	if ( mode(message) != "raw" )
		stop ("byte array message expected.")
	
	sapply ( as.list(descriptor$getFields()), function (fd) {
				1
			} )
}



proto.toProtoBldr <- function ( x, descriptor ) {
	x<- as.list(x)
	bldr <- J("com.google.protobuf.DynamicMessage")$newBuilder(descriptor)
	
	sapply( names(x), function (fname ) {
				fd <- descriptor$findFieldByName(fname)
				if ( length(fd) == 0 ) 
					stop (sprintf("Unable to find field descriptor for '%s'.",fname))
				else {
					if ( fd$isRepeated() )
						bldr$addRepeatedField(fd, .proto.parseRepeatedField(x[[fname]],fd))
					else 
						bldr$setField(fd, .proto.parseNonrepeatedField(x[[fname]],fd))
				} 
				
				
			})
}

