@echo off
call ant build
call ant UnitAndPerformanceTests
call ant junitreport
call junit\index.html
