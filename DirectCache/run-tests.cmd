@echo off
call ant build
call ant PerformanceWithSmallBuffersTest
call ant junitreport
call junit\index.html
