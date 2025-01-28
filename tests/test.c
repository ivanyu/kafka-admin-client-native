#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "wrapper.h"

int main(int argc, char **argv) {
    graal_isolate_t *isolate = NULL;
    graal_isolatethread_t *thread = NULL;

    if (graal_create_isolate(NULL, &isolate, &thread) != 0) {
        fprintf(stderr, "initialization error\n");
        return 1;
    }

    printf("Calling experiment (C side)\n");
    const char* const string = "Hello world";
    experiment(thread, string);

    graal_tear_down_isolate(thread);
}
