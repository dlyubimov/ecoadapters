# TODO: Add comment
# 
# Author: dmitriy
###############################################################################




test1 <- function () {

  	
}

testSW <- function() { 
	library(ecor)
	sfw <- ecor.SequenceFileW$new("/temp/swftest1.seq")
	tryCatch({
				#so vectorizaton and recycling
				# is now supported by append. 
				# we can write batches from vectors at once. 
					system.time(sfw$append(rep("KEYKEY", length=3000),"BBVAL"))
			}, 
			finally = {
				sfw$close()
			}
			)
	
}

test2 <- function () {
  	library(ecor)
	library(compiler)
	
	d <- proto.desc('com.inadco.ecoadapters.ecor.tests.codegen.Tests$EcorTest')

	rmsg <- list()
	rmsg$idbuff <- charToRaw('012345566')
	rmsg$str <- 'this is a string'
	
	# can use either atomic vector or a list with integral 
	# subscripting
	rmsg$rvector <- c(5,2)
	rmsg$rvector2 <- list()
	rmsg$rvector2[[1]] <- 1
	rmsg$rvector2[[2]] <- 2
	rmsg$timeAttr <- as.numeric(Sys.time())*1000
	rmsg$nested1 <- list()
	rmsg$nested1$name <- "nested1"
    rmsg$nested2 <- list()
	rmsg$nested2[[1]] <- list()
	rmsg$nested2[[1]]$name <- "nested2-1"
    rmsg$nested2[[2]]<-list()
	rmsg$nested2[[2]]$name <- "nested2-2"
	rmsg$floatval <- 32.0
	rmsg$intval <- 32
	rmsg$boolval <- T
  
  

	p <- proto.toProtoBldr( rmsg, d )
	praw <- proto.toProtoRaw( rmsg, d)
	rl <- proto.fromProtoRaw(praw,d)
	
	# will use as.list() coersion from a proxy form
	lapply(rl, function(x) x)
	
	# perhaps better way without list coersion 
	# and still conversion on demand
	lapply(names(rl), function(x) rl[[x]] )

	# and now back again to byte array
	p1 <- proto.toProtoRaw(rl,d)
	
	
	system.time({for (i in 1:1000) praw <- proto.toProtoRaw( rmsg, d)})
	

	system.time({for (i in 1:1000)  rl <- proto.fromProtoRaw(praw,d)})
	system.time({for (i in 1:1000)  rl <- proto.fromProtoRaw(praw,d,F)})
	
	e <- compile(for (i in 1:1000)  rl <- proto.fromProtoRaw(praw,d))
	system.time(eval(e))

	e <- compile(for (i in 1:1000)  rl <- proto.fromProtoRaw(praw,d,F))
	system.time(eval(e))
	
	
	names(rl)
	class(rl)
	

  #	expect_that(rawToChar(rl$idbuff),equals('012345566'))
  # expect_that(rl$clickThru$advertiserAccountNumber, equals('this is a string'))
  
  valW <- ecor.ProtoWritable$new(
      'com.inadco.ecoadapters.ecor.tests.codegen.Tests$EcorTest')
  
  infile <- "/temp/swftest1.seq"
	sfw <- ecor.SequenceFileW$new(infile,
			valWritable=valW)
	
  tryCatch({
				#so vectorizaton and recycling
				# is now supported by append. 
				# we can write batches from vectors at once.
				for ( i in 1:1000)	sfw$append("",rmsg)
			}, 
			finally = {
				sfw$close()
			}
	)
  
   mapsetupfun <- function() {
	 d<<- proto.desc('com.inadco.ecoadapters.ecor.tests.codegen.Tests$EcorTest')
   }
	
  mapfun <- function ( key,value ) { 
	rl <- proto.fromProtoRaw(value, d)
	ecor.collect(rl$nested1$name, rl$nested1$name)
	ecor.collect(rl$nested1$name, rl$nested2[[2]]$name)
	stop ("from map.")
  }
  
  reducefun <- function (key, vals ) {
	  ecor.collect(key,vals)
	  stop(sprintf("collected %d records.",length(vals)))
  }
  
  hconf <- ecor.HConf$new()
  hconf$setInput( infile )
  hconf$setOutput("/temp/rmr-out")
  
  hconf$setMapper(mapfun)
  hconf$setMapSetup(mapsetupfun)
  hconf$setReducer(reducefun)
  hjob <- hconf$mrSubmit(T)
  hjob$waitForCompletion()
  
}

#context("prototests")
#test_that("protoconversions",test2())


