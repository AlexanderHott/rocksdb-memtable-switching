# Performance Improvements

Initial times

| Workload    | Time (s) | 
|-------------|---------:|
| 1m_i__1m_u  |   5.3982 |
| 1m_i__1m_d  |  3.4613* |
| 1m_i__1m_pq |   3.8674 |
| 1m_i__1m_rq | too long |

\* Did not generate correct, so not a valid comparison

## Switching to `String` to `Box<[u8]`

Layout of a `String`.

```
+-----------------------+
| ptr 8 | len 8 | cap 8 |
+-+---------------------+
  |
+-v--------------------+
| utf8 rune 8-32 | ... |
+----------------------+
```

Layout of a `Box<[u8]>`.

```
+---------------+
| ptr 8 | len 8 |
+-+-------------+
  |
+-v------+-----+
| byte 8 | ... |
+--------+-----+
```

- also removed global operation vec in favor of passing a `impl Write`.
    - That way, a user could pass a `BufWriter`, `Vec`, or `sink()`

| Workload    | Time (s) |   Diff % |
|-------------|---------:|---------:|
| 1m_i__1m_u  |   3.6082 | -33.159% |
| 1m_i__1m_d  | too long |       -- |
| 1m_i__1m_pq |   2.4160 | -37.529% |
| 1m_i__1m_rq | too long |       -- |

- deletes cause a shift of the entire array, so it's too slow
    - use a `Vec<Option<Box<[u8]>>>` instead
- range queries + inserts require dynamic sorted keys, so it is just a slow operation