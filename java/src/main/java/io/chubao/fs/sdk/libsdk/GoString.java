package io.chubao.fs.sdk.libsdk;

import com.sun.jna.Structure;

import java.util.ArrayList;
import java.util.List;

public class GoString extends Structure {
	public String ptr;
	public long len;

	public GoString() {
	}

	public GoString(String str) {
		this.ptr = str;
		this.len = str.length();
	}

	@Override
	protected List<String> getFieldOrder() {
		List<String> fields = new ArrayList<>();
		fields.add("ptr");
		fields.add("len");
		return fields;
	}

	public static class ByValue extends GoString implements Structure.ByValue {
		public ByValue() {
		}

		public ByValue(String str) {
			super(str);
		}
	}

	public static class ByReference extends GoString implements Structure.ByReference {
		public ByReference() {
		}

		public ByReference(String str) {
			super(str);
		}
	}
}
