@echo off
@echo creating snapshot
mvn -DaltDeploymentRepository=snapshot-repo::default::file:..\Misc\mvn-repo\snapshots clean deploy 
