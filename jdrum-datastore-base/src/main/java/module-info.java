module jdrum.datastore.base {
    requires java.base;
    requires org.slf4j;
    requires jdrum.commons;
    requires jsr305;

    exports at.rovo.drum.base;
}