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