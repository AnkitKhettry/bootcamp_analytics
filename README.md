# Bootcamp Analytics

Pre-requisites:
Must have intellij, java 8, git installed.

Steps:
1. Clone the repo : https://github.com/AnkitKhettry/bootcamp_analytics.git
2. Goto File -> Project Structure -> Platform Settings -> SDK -> Add 1.8s, remove all others.
3. Goto File -> Project Structure -> Libraries -> + Scala SDK -> Add 2.11.7, remove all others. If you don't have, download:https://www.scala-lang.org/download/2.11.7.html
4. Add pom.xml to your maven projects. Do a mvn clean and then mvn install. Wait for imports.

Exercise 1:
1. Listen to code walkthrough
2. Run KafkaWriter.main
3. Run SessionCount.main

Exercise 2:
Create a new feature branch on your local system, and implement a spark streaming application which would find the number of 