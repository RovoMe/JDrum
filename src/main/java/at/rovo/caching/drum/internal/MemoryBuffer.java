package at.rovo.caching.drum.internal;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *
 */
public class MemoryBuffer<T>
{
	private final ConcurrentLinkedQueue<T> buffer;
	private int byteLengthKV;
	private int byteLengthAux;

	public MemoryBuffer(ConcurrentLinkedQueue<T> buffer, Integer byteLengthKV, Integer byteLengthAux)
	{
		this.buffer = buffer;
		this.byteLengthKV = byteLengthKV;
		this.byteLengthAux = byteLengthAux;
	}

	public ConcurrentLinkedQueue<T> getBuffer() {
		return buffer;
	}

	public int getByteLengthKV() {
		return byteLengthKV;
	}

	public void setByteLengthKV(int byteLengthKV) {
		this.byteLengthKV = byteLengthKV;
	}

	public int getByteLengthAux() {
		return byteLengthAux;
	}

	public void setByteLengthAux(int byteLengthAux) {
		this.byteLengthAux = byteLengthAux;
	}

	public void addByteLengthKV(int byteLengthKV) {
		this.byteLengthKV += byteLengthKV;
	}

	public void addByteLengthAux(int byteLengthAux) {
		this.byteLengthAux += byteLengthAux;
	}
}
