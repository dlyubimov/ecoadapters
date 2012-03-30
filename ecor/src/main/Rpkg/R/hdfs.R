
hdfs.dfs <- function( paths ) 
	J("org.apache.hadoop.fs.FileSystem")$get(ecor$jconf)	

# convert string to Path 
hdfs.path <- function(parent, child = NULL) {
	if ( length(child) == 0 )
		new ( J("org.apache.hadoop.fs.Path"), parent )
	else 
		new ( J("org.apache.hadoop.fs.Path"),
				.ecor.jpath(parent),child)
}

#local fs
hdfs.localfs <- function () 
	J("org.apache.hadoop.fs.FileSystem")$getLocal(ecor$jconf)

hdfs.delete <- function (fs, paths, recursive=T ) {
	# as a vector of charater
	if ( class(paths)=="character") { 
		for (p in paths ) fs$delete(hdfs.path(p),recursive)

	} else if ( as.character(class(paths))=="jobjRef") {
		if ( "org.apache.hadoop.fs.Path"==.jclass(p) ) {
			fs$delete(p,recursive)
		} else {
			stop (sprintf ("unable how to handle parameter of class %s",.jclass(p)) )
		} 
	} else if (as.character(paths)=="list") {
		sapply(paths, function(x) { hdfs.delete(fs,x,recursive); NULL} )
		
	} else stop ("invalid paths parameter")
}


