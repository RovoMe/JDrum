JDrum
=====

Java implementation of the disk repository with update management (DRUM) framework as presented by Hsin-Tsang Lee, Derek Leonard, Xiaoming Wang, and Dmitri Loguinov in the paper "IRLbot: Scaling to 6 Billion Pages and Beyond"

DRUM divides data received via its check, checkUpdate or update method into a number of buckets and stores them efficiently with the help of bucket sort into bucket disk files. If a disk files gets full all the data is stored and compared with the data in a general backend data store and updated if required.
