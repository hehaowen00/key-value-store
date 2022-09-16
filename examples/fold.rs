use key_value_store::{Bitcask, Options};

fn main() {
    let opts = Options::new("./fold-kv");
    let mut kv = Bitcask::open(opts);

    kv.insert_if_none(b"person:1:name", b"alice").unwrap();
    kv.insert_if_none(b"person:1:age", b"1").unwrap();

    kv.insert_if_none(b"person:2:name", b"blob").unwrap();
    kv.insert_if_none(b"person:2:age", b"2").unwrap();

    kv.insert_if_none(b"person:3:name", b"john").unwrap();
    kv.insert_if_none(b"person:3:age", b"3").unwrap();

    kv.flush();

    let count = kv.fold(
        |k, _v, acc| {
            if k.starts_with(b"person") && k.ends_with(b"age") {
                return acc + 1;
            }
            acc
        },
        0,
    );

    println!("number of contacts: {}", count);
}
