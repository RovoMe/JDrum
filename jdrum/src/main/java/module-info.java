module jdrum {
    requires java.base;
    requires org.slf4j;
    requires jcip.annotations;

    requires transitive jdrum.datastore.base;
    requires transitive jdrum.commons;
}