#!/bin/bash

# compiles R stuff. 
# had problems with putting this into 
# maven antrun.
# param #1 -> R pkg version
# param #2 -> build version 
# param #3 -> output pkgdir

if [ $# -ne 3 ]; then 
  echo 'usage: genDescription.sh <r-package> <maven-version> <output-DESCRIPTION-file-path>'
  exit 1
fi

test ! -f "`which R`" && { echo "Cannot find R. is R installed? In the path?"; exit 1; }

pkgName=$1
mver=$2
pkgdir=$3
descrFile=$pkgdir/DESCRIPTION

d=`date +%F`

# strip -SNAPSHOT things that create problems 
rver=`sed 's/-SNAPSHOT\$/-00000000/' <<< $mver`

echo $ver

sed "s/^Version:/& ${rver}/" DESCRIPTION.tmpl | sed "s/^Date:/& ${d}/" | \
sed "s/^Package:/& ${pkgName}/" > "$descrFile" 

# also while we are at it, try to generate manuals using roxygen2 package

R --vanilla <<EOF

library(roxygen2)
roxygenize(package.dir="${pkgdir}",roclets="rd")

#library(roxygen)
#roxygenize(package.dir="${pkgdir}")

q()

EOF
