package at.rovo.caching.drum.event;

public enum InMemoryBufferState
{
    EMPTY,
    WITHIN_LIMIT,
    EXCEEDED_LIMIT,
    STOPED
}
