//! Integration tests for the storage layer.

use qonduit_storage::{HotCache, WarmStorage};

fn create_test_storage() -> WarmStorage {
    let dir = tempfile::tempdir().unwrap();
    WarmStorage::open(dir.path()).unwrap()
}

#[test]
fn test_tick_roundtrip() {
    let storage = create_test_storage();
    let data = b"{\"tick\":42,\"epoch\":1}";

    storage.put_tick(42, data).unwrap();
    let retrieved = storage.get_tick(42).unwrap();
    assert_eq!(retrieved, Some(data.to_vec()));

    // Non-existent tick
    assert!(storage.get_tick(999).unwrap().is_none());
}

#[test]
fn test_current_tick_meta() {
    let storage = create_test_storage();

    assert!(storage.get_current_tick().unwrap().is_none());

    storage.set_current_tick(1000).unwrap();
    assert_eq!(storage.get_current_tick().unwrap(), Some(1000));

    storage.set_current_tick(2000).unwrap();
    assert_eq!(storage.get_current_tick().unwrap(), Some(2000));
}

#[test]
fn test_current_epoch_meta() {
    let storage = create_test_storage();

    assert!(storage.get_current_epoch().unwrap().is_none());

    storage.set_current_epoch(42).unwrap();
    assert_eq!(storage.get_current_epoch().unwrap(), Some(42));
}

#[test]
fn test_tx_roundtrip() {
    let storage = create_test_storage();
    let hash = [1u8; 32];
    let data = b"{\"hash\":\"abc\",\"amount\":1000}";

    storage.put_tx(&hash, data).unwrap();
    let retrieved = storage.get_tx(&hash).unwrap();
    assert_eq!(retrieved, Some(data.to_vec()));
}

#[test]
fn test_entity_roundtrip() {
    let storage = create_test_storage();
    let key = [0x42u8; 32];
    let data = b"{\"identity\":\"ABC\",\"incoming\":1000}";

    storage.put_entity(&key, data).unwrap();
    let retrieved = storage.get_entity(&key).unwrap();
    assert_eq!(retrieved, Some(data.to_vec()));
}

#[test]
fn test_computors_roundtrip() {
    let storage = create_test_storage();
    let data = b"{\"epoch\":42,\"public_keys\":[]}";

    storage.put_computors(42, data).unwrap();
    let retrieved = storage.get_computors(42).unwrap();
    assert_eq!(retrieved, Some(data.to_vec()));

    assert!(storage.get_computors(99).unwrap().is_none());
}

#[test]
fn test_hot_cache_tick() {
    let cache = HotCache::new(100, 100);

    assert!(cache.get_tick(1).is_none());

    cache.put_tick(1, vec![1, 2, 3]);
    assert_eq!(cache.get_tick(1), Some(vec![1, 2, 3]));
}

#[test]
fn test_hot_cache_current_tick() {
    let cache = HotCache::new(100, 100);

    assert!(cache.get_current_tick().is_none());

    cache.set_current_tick(500);
    assert_eq!(cache.get_current_tick(), Some(500));
}

#[test]
fn test_tick_range() {
    let storage = create_test_storage();

    for tick in 10..20 {
        let data = format!("{{\"tick\":{tick}}}");
        storage.put_tick(tick, data.as_bytes()).unwrap();
    }

    // get_tick_range is inclusive of both endpoints
    let range = storage.get_tick_range(12, 16).unwrap();
    assert_eq!(range.len(), 5); // 12, 13, 14, 15, 16
    assert_eq!(range[0].0, 12);
    assert_eq!(range[4].0, 16);
}
