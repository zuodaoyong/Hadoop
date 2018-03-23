package com.functions;

import java.io.InputStream;

@FunctionalInterface
public interface ReadFile<T> {
	T read(InputStream inputStream);
}
