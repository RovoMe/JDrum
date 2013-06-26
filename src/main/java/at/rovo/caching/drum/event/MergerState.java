package at.rovo.caching.drum.event;

public enum MergerState
{
	WAITING_ON_MERGE_REQUEST,
	MERGE_REQUESTED,
	WAITING_ON_LOCK,
	MERGING,
	FINISHED_WITH_ERRORS,
	FINISHED
}
