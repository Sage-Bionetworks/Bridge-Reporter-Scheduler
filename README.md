# Bridge-Reporter-Scheduler
Bridge Reporter Scheduler which runs on AWS Lambda. This is the Lambda handler that sends messages to Bridge-Reporter's SQS
request queue

To run a full build (including compile, unit tests, findbugs, and jacoco test coverage), run:
mvn verify

(A full build takes about 15 seconds on my laptop, from a clean workspace.)

To just run findbugs, run:
mvn compile findbugs:check

To run findbugs and get a friendly GUI to read about the bugs, run:
mvn compile findbugs:findbugs findbugs:gui

To run jacoco coverage reports and checks, run:
mvn test jacoco:report jacoco:check

Jacoco report will be in target/site/jacoco/index.html

To test locally
mvn compile exec:java -Dexec.mainClass=org.sagebionetworks.bridge.reporter.scheduler.SchedulerLauncher -Dexec.args=[scheduler name]

The "compile" is important because otherwise exec:java may execute a stale version of your code

To deploy, after running mvn verify, upload target/Bridge-Reporter-Scheduler-1.0.jar to AWS Lambda in the AWS console.
(Travis doesn't yet support Java deployments to Lambda. See https://github.com/travis-ci/dpl/issues/397)
