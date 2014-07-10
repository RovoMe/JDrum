package at.rovo.caching.drum.data;

import at.rovo.caching.drum.util.DrumUtil;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class ObjectSerializer<O> implements ByteSerializer<ObjectSerializer<O>>
{
	private O object;
	private Class<? super O> objectClass;

	public ObjectSerializer(O object, Class<? super O> objectClass)
	{
		this.object = object;
		this.objectClass = objectClass;
	}

	public O getObject()
	{
		return this.object;
	}

	@Override
	public byte[] toBytes()
	{
		int size;
		int featureLenght;
		byte[] objectBytes;
		if (object != null)
		{
			objectBytes = this.convertToBytes(object);
			featureLenght = objectBytes.length;
			size = 4+featureLenght;
		}
		else
		{
			objectBytes = new byte[0];
			featureLenght = 0;
			size = 4;
		}
		byte[] totalBytes = new byte[size];
		int pos = 0;
		// write the length of the object
		System.arraycopy(DrumUtil.int2bytes(featureLenght), 0, totalBytes, pos, pos += 4);
		// write the object if it is available
		if (object != null)
		{
			System.arraycopy(objectBytes, 0, totalBytes, pos, featureLenght);
		}

		return totalBytes;
	}

	@Override
	@SuppressWarnings("unchecked")
	public ObjectSerializer<O> readBytes(byte[] data)
	{
		// read the lenght of the Object
		byte[] objectLengthBytes = new byte[4];
		System.arraycopy(data, 0, objectLengthBytes, 0, 4);
		int objectLength = DrumUtil.bytes2int(objectLengthBytes);
		// read the object
		byte[] objectBytes = new byte[objectLength];
		System.arraycopy(data, 4, objectBytes, 0, objectLength);
		O object = ((O)this.convertBytesToObject(objectBytes, objectClass));

		// create and return a new instance of this class
		return new ObjectSerializer<>(object, objectClass);
	}

	private byte[] convertToBytes(Object obj)
	{
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream())
		{
			try (ObjectOutputStream oos = new ObjectOutputStream(baos))
			{
				oos.writeObject(obj);
				return baos.toByteArray();
			}
			catch (IOException ioEx)
			{
				throw new RuntimeException("Could not serialize object", ioEx);
			}
		}
		catch (IOException ioEx)
		{
			throw new RuntimeException("Could not convert object to byte[]", ioEx);
		}
	}

	private <T> T convertBytesToObject(byte[] bytes, Class<T> typeClass)
	{
		try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes)))
		{
			return typeClass.cast(ois.readObject());
		}
		catch (IOException | ClassNotFoundException ex)
		{
			throw new RuntimeException("Could not serialize bytes back to object", ex);
		}
	}
}
