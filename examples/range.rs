use key_value_store::{Bitcask, Options};

fn main() {
    let opts = Options::new("./range-kv");
    let mut kv = Bitcask::open(opts);

    kv.insert(&id(1), b"a").unwrap();
    kv.insert(&id(2), b"b").unwrap();
    kv.insert(&id(3), b"c").unwrap();
    kv.insert(&id(4), b"d").unwrap();
    kv.insert(&id(5), b"e").unwrap();

    let range = id(2).to_vec()..;

    for (k, v) in kv.iter(range) {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&k);
        println!("{} {}", u64::from_be_bytes(buf), to_str(v.data()));
    }
}

fn id(v: u64) -> [u8; 8] {
    v.to_be_bytes()
}

fn to_str(b: &[u8]) -> &str {
    unsafe { std::str::from_utf8_unchecked(b) }
}
