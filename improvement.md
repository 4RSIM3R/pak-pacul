## value.rs

Choose unix-timestamp as base for handling timestamp or date related data types, only introduce very primitive data types
only support : 

* Null
* Integer
* Real (Float)
* Text 
* Blob
* Boolean
* Timestamp

## row.rs

Using custom binary format instead of bincode, preventing wasting ser-de bytes processed, here the comparison result : 

```txt
---- types::row_test::demonstrate_serialization_waste bincode ----
Calculated size: 21 bytes
Actual serialized size: 30 bytes
Waste ratio: 1.43x

---- types::row_test::demonstrate_serialization_waste custom_binary_format ----
Calculated size: 17 bytes
Actual serialized size: 17 bytes
Waste ratio: 1.00x
```

bincode config i use : 

```rust
let config = bincode::config::standard()
            .with_fixed_int_encoding()
            .with_little_endian();
```

Because bincode add :
- Vec length prefix (8 bytes)
- Option discriminant (1 byte)
- Enum discriminants for each Value
- Potential padding/alignment

## page.rs

Introducing slot pointer array, that will manage the position of row / cell inside page, so with that one, we can
load only metadata needed such as, page_id, that slot pointer array and next page id, instead of full 4KB page on memory
then pass-it to global worker queue, and the remain row reading process can be lazily implemented later on the worker side

---- types::page_test::test_metadata_only_mode (test_metadata_only_mode) ----
Metadata size: 44
Memory footprint: 224
Page size: 4096

See, instead of load that 4KB, we can only load -+224 bytes to memory