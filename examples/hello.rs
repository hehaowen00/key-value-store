use key_value_store::{Bitcask, Options};

fn main() {
    let opts = Options::new("./hello-kv");
    let mut kv = Bitcask::open(opts);

    let res = kv.insert_if_none(b"message", b"Hello, World!");
    println!("{:?}", res);

    kv.flush();

    let res = kv.get(b"message");

    if let Some(value) = res {
        println!("{}", to_str(value.data()));
    }
}

fn to_str(bytes: &[u8]) -> &str {
    unsafe { std::str::from_utf8_unchecked(bytes) }
}
