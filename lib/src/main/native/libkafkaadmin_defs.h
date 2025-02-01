#ifndef __KAFKA_ADMIN_STRUCTS
#define __KAFKA_ADMIN_STRUCTS

typedef struct __key_value_t {
  const char * const key;
  const char * const value;
} key_value_t;

typedef struct __node_t {
    int id;
    const char * host;
    int port;
    const char * rack;
} node_t;

typedef struct __describe_cluster_result_t {
    int num_nodes;
    const node_t * nodes;
    const node_t * controller;
    const char * cluster_id;
    int num_authorized_operations;
    const char * authorized_operations;
} describe_cluster_result_t;

typedef struct __new_topic_t {
    const char * name;
    int num_partitions;
    short replication_factor;
    // TODO add replica assignments
    // TODO add configs
} new_topic_t;

typedef struct __create_topic_result_t {
    const char * topic;
    const char * error;
    const char * uuid;
    int num_partitions;
    int replication_factor;
    // TODO add config
} create_topic_result_t;

typedef struct __create_topics_result_t {
    int num_topics;
    const create_topic_result_t * topics;
} create_topics_result_t;

#endif
