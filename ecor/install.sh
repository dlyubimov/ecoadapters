# compile AND install R package. 
# need to have R, rJava  and have a sudo 
# for this to work. Also have maven executable around

MVN='mvn clean install -DskipTests -DR'

sudo R CMD REMOVE ecor; { $MVN && sudo HADOOP_HOME=$HADOOP_HOME R CMD INSTALL --build target/ecor-0.4.0-SNAPSHOT-rpkg; }
