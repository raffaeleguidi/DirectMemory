@echo off
call ant build
call ant PerformanceTest
call ant junitreport
call junit\index.html
