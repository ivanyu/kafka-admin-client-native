#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

#[cfg(test)]
mod tests {
    use super::*;
    use static_init::dynamic;
    use std::collections::{HashMap, HashSet};
    use std::ffi::{c_int, CStr, CString};
    use std::os::raw::c_void;
    use std::process::Command;
    use std::ptr::{null, null_mut};
    use std::thread::{sleep, Thread};
    use std::time::Duration;

    const KAFKA_PORT_1: u16 = 19092;
    const KAFKA_PORT_2: u16 = 29092;

    struct KafkaCluster {}

    impl KafkaCluster {
        fn new() -> KafkaCluster {
            KafkaCluster::clean();

            let output = Command::new("docker")
                .args(["compose", "up", "--detach", "--wait"])
                .output()
                .unwrap();
            assert!(output.status.success());
            KafkaCluster {}
        }

        fn bootstrap_servers(&self) -> String {
            format!("127.0.0.1:{KAFKA_PORT_1},127.0.0.1:{KAFKA_PORT_2}")
        }

        fn clean() {
            let output = Command::new("docker")
                .args(["compose", "down", "--remove-orphans"])
                .output()
                .unwrap();
            assert!(output.status.success());
        }
    }

    impl Drop for KafkaCluster {
        fn drop(&mut self) {
            KafkaCluster::clean();
        }
    }

    #[dynamic(lazy, drop)]
    static mut KAFKA_CLUSTER: KafkaCluster = KafkaCluster::new();

    #[test]
    fn error_when_creating_client_must_result_in_null() {
        unsafe {
            let thread = create_isolate();

            let kvs: [key_value_t; 0] = [];
            let handle = create_admin_client(thread, kvs.len() as c_int, kvs.as_ptr());
            assert!(handle.is_null());

            let bootstrap_server_key = CString::new("bootstrap.servers_XXX").unwrap();
            let bootstrap_server_value = CString::new("".to_owned()).unwrap();
            let kvs = [key_value_t {
                key: bootstrap_server_key.as_ptr(),
                value: bootstrap_server_value.as_ptr(),
            }];
            let handle = create_admin_client(thread, kvs.len() as c_int, kvs.as_ptr());
            assert!(handle.is_null());

            tear_down_isolate(thread);
        }
    }

    #[test]
    fn deleting_non_existing_client_must_not_cause_failure() {
        unsafe {
            let thread = create_isolate();
            delete_admin_client(thread, null());
            delete_admin_client(thread, 10 as *const c_void);
            tear_down_isolate(thread);
        }
    }

    #[test]
    fn test_describe_cluster() {
        unsafe {
            let thread = create_isolate();
            let handle = create_client(thread);

            let cluster = describe_cluster(thread, handle);

            assert_eq!((*cluster).num_nodes, 2);

            let mut node_id_found: HashSet<u32> = HashSet::new();
            for i in 0..(*cluster).num_nodes {
                let node = (*cluster).nodes.offset(i.try_into().unwrap());

                node_id_found.insert((*node).id as u32);

                assert_eq!(CStr::from_ptr((*node).host).to_str().unwrap(), "127.0.0.1");
                if (*node).id == 1 {
                    assert_eq!((*node).port, 19092);
                    assert_eq!(CStr::from_ptr((*node).rack).to_str().unwrap(), "rack1");
                } else {
                    assert_eq!((*node).port, 29092);
                    assert_eq!(CStr::from_ptr((*node).rack).to_str().unwrap(), "rack2");
                }
            }
            assert_eq!(node_id_found, HashSet::from([1, 2]));

            let controller = (*cluster).controller;
            assert!((*controller).id == 1 || (*controller).id == 2);
            assert_eq!(
                CStr::from_ptr((*controller).host).to_str().unwrap(),
                "127.0.0.1"
            );
            assert!((*controller).port == 19092 || (*controller).port == 29092);
            assert!(
                CStr::from_ptr((*controller).rack).to_str().unwrap() == "rack1"
                    || CStr::from_ptr((*controller).rack).to_str().unwrap() == "rack2"
            );
            assert_eq!(
                CStr::from_ptr((*cluster).cluster_id).to_str().unwrap(),
                "5L6g3nShT-eMCtK--X86sw"
            );
            assert_eq!((*cluster).num_authorized_operations, 0);
            assert!((*cluster).authorized_operations.is_null());

            free_describe_cluster_result(thread, cluster);

            delete_admin_client(thread, handle);
            tear_down_isolate(thread);
        }
    }

    #[test]
    fn describe_cluster_invalid_handler() {
        unsafe {
            let thread = create_isolate();
            assert!(describe_cluster(thread, null()).is_null());
            assert!(describe_cluster(thread, 10 as *const c_void).is_null());
            tear_down_isolate(thread);
        }
    }

    #[test]
    fn test_create_list_topics() {
        unsafe {
            let thread = create_isolate();
            let handle = create_client(thread);

            let topic1_name = CString::new("topic1").unwrap();
            let topic2_name = CString::new("topic2").unwrap();
            let new_topics: [new_topic_t; 2] = [
                new_topic_t {
                    name: topic1_name.as_ptr(),
                    num_partitions: 2,
                    replication_factor: 1,
                },
                new_topic_t {
                    name: topic2_name.as_ptr(),
                    num_partitions: 1,
                    replication_factor: 2,
                },
            ];
            let mut result = create_topics(
                thread,
                handle,
                new_topics.len() as c_int,
                new_topics.as_ptr(),
            );

            assert_eq!((*result).num_topics, 2);

            let created_topic1 = (*result).topics.offset(0);
            assert_eq!(
                CStr::from_ptr((*created_topic1).topic).to_str().unwrap(),
                "topic1"
            );
            if !(*created_topic1).error.is_null() {
                println!(
                    "Error: {}",
                    CStr::from_ptr((*created_topic1).error).to_str().unwrap()
                );
            }
            assert!((*created_topic1).error.is_null());
            assert!(!(*created_topic1).uuid.is_null());
            let created_topic1_id = CStr::from_ptr((*created_topic1).uuid)
                .to_str()
                .unwrap()
                .to_string();
            assert_eq!((*created_topic1).num_partitions, 2);
            assert_eq!((*created_topic1).replication_factor, 1);

            let created_topic2 = (*result).topics.offset(1);
            assert_eq!(
                CStr::from_ptr((*created_topic2).topic).to_str().unwrap(),
                "topic2"
            );
            if !(*created_topic2).error.is_null() {
                println!(
                    "Error: {}",
                    CStr::from_ptr((*created_topic2).error).to_str().unwrap()
                );
            }
            assert!((*created_topic2).error.is_null());
            assert!(!(*created_topic2).uuid.is_null());
            let created_topic2_id = CStr::from_ptr((*created_topic2).uuid)
                .to_str()
                .unwrap()
                .to_owned();
            assert_eq!((*created_topic2).num_partitions, 1);
            assert_eq!((*created_topic2).replication_factor, 2);

            free_create_topics_result(thread, result);

            sleep(Duration::from_secs(1));

            let mut result = list_topics(thread, handle);

            assert_eq!((*result).num_topics, 2);

            let listed_topic1 = (*result).topics.offset(0);
            assert_eq!(
                CStr::from_ptr((*listed_topic1).name).to_str().unwrap(),
                "topic1"
            );
            assert_eq!(
                CStr::from_ptr((*listed_topic1).topic_id).to_str().unwrap(),
                created_topic1_id
            );
            assert!(!(*listed_topic1).is_internal);

            let listed_topic2 = (*result).topics.offset(1);
            assert_eq!(
                CStr::from_ptr((*listed_topic2).name).to_str().unwrap(),
                "topic2"
            );
            assert_eq!(
                CStr::from_ptr((*listed_topic2).topic_id).to_str().unwrap(),
                created_topic2_id
            );
            assert!(!(*listed_topic2).is_internal);

            free_list_topics_result(thread, result);

            delete_admin_client(thread, handle);
            tear_down_isolate(thread);
        }
    }

    unsafe fn create_isolate() -> *mut graal_isolatethread_t {
        let mut isolate: *mut graal_isolate_t = null_mut();
        let mut thread: *mut graal_isolatethread_t = null_mut();
        if graal_create_isolate(
            null_mut::<graal_create_isolate_params_t>(),
            &mut isolate as *mut _,
            &mut thread as *mut _,
        ) != 0
        {
            panic!("Error creating isolate");
        }
        thread
    }

    fn tear_down_isolate(thread: *mut graal_isolatethread_t) {
        unsafe {
            if graal_tear_down_isolate(thread) != 0 {
                panic!("Isolate tear down error");
            }
        }
    }

    fn create_client(thread: *mut graal_isolatethread_t) -> *mut c_void {
        let bootstrap_server_key = CString::new("bootstrap.servers").unwrap();
        let bootstrap_server_value =
            CString::new(KAFKA_CLUSTER.read().bootstrap_servers()).unwrap();
        let kvs = [key_value_t {
            key: bootstrap_server_key.as_ptr(),
            value: bootstrap_server_value.as_ptr(),
        }];
        unsafe { create_admin_client(thread, kvs.len() as c_int, kvs.as_ptr()) }
    }
}
