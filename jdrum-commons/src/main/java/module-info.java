module jdrum.commons {
    requires java.base;
    requires jsr305;

    exports at.rovo.drum;
    exports at.rovo.drum.data;
    exports at.rovo.drum.datastore;
    exports at.rovo.drum.event;
    exports at.rovo.drum.util;
    exports at.rovo.drum.util.lockfree to jdrum;
}