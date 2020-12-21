module jdrum.datastore.simple {
    requires java.base;
    requires org.slf4j;
    requires jdrum.commons;
    requires jdrum.datastore.base;
    requires jsr305;

    exports at.rovo.drum.datastore.simple;
}