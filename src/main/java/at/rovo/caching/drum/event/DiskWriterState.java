package at.rovo.caching.drum.event;

public enum DiskWriterState
{
	EMPTY,
	WAITING_ON_DATA,
	DATA_RECEIVED,
	WAITING_ON_LOCK,
	WRITING,
	FINISHED,
	FINISHED_WITH_ERROR
}
