package io.chubao.fs.sdk.libsdk;

import com.sun.jna.Structure;

import java.util.ArrayList;
import java.util.List;

public class GoBuffer extends Structure {
	public byte[] ptr;
	//public long len;

	public GoBuffer() {
	}

	public GoBuffer(byte[] str) {
		this.ptr = str;
		//this.len = str.length;
	}

	@Override
	protected List<String> getFieldOrder() {
		List<String> fields = new ArrayList<>();
		fields.add("ptr");
		//fields.add("len");
		return fields;
	}

	public static class ByValue extends GoBuffer implements Structure.ByValue {
		public ByValue() {
		}

		public ByValue(byte[] str) {
			super(str);
		}
	}

	public static class ByReference extends GoBuffer implements Structure.ByReference {
		public ByReference() {
		}

		public ByReference(byte[] str) {
			super(str);
		}
	}
}
