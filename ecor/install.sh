# compile AND install R package. 
# need to have R, rJava  and have a sudo 
# for this to work. Also have maven executable around

MVN='mvn -o clean install -DskipTests -DR'
VER=`mvn help:evaluate -Dexpression=project.version | grep -vi "\\(\\[INFO\\]\\)\\|\\(\\[WARNING\\]\\)"`

PKG="target/ecor-${VER}-rpkg"

echo installing R package $PKG

sudo R CMD REMOVE ecor; { $MVN && sudo HADOOP_HOME=$HADOOP_HOME R_COMPILE_PKGS=1 R CMD INSTALL --build $PKG; }
