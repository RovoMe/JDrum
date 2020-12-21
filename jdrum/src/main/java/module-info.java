/**
 * This module contains the concrete implementation for the <em>Disk Repository with Update Management (DRUM)</em>
 * framework.
 */
module jdrum {
    requires java.base;
    requires org.slf4j;
    requires jcip.annotations;
    requires jsr305;

    requires transitive jdrum.datastore.base;
    requires transitive jdrum.commons;
}