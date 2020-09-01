#!/bin/bash


#mvn clean package -pl solrwriter/ -am -DskipTests
#cp /Users/wildpea/git/alibaba/DataX/solrwriter/target/solrwriter-0.0.1-SNAPSHOT.jar ~/


#cd /Users/wildpea/git/alibaba/DataX/solrwriter/target/datax/plugin/writer/
#tar zcvf solrwriter.tar.gz ./solrwriter
#cp solrwriter.tar.gz ~/


mvn clean package -pl hdfsreader/ -am -DskipTests
cp /Users/wildpea/git/alibaba/DataX/hdfsreader/target/hdfsreader-0.0.1-SNAPSHOT.jar ~/
