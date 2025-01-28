LIBDIR=../lib/build/native/nativeCompile/

test: test.c
	gcc $< -o $@ -I $(LIBDIR) -L $(LIBDIR) -l kafkaadmin -Wl,-rpath $(LIBDIR)
