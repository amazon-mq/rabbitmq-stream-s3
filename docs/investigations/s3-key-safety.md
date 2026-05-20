# S3 Key Safety: User-Controlled Input in Object Keys

## Context

In a managed service context, operators are trusted and end-users are untrusted. Users control vhost names and queue names via AMQP/Streams protocols. These flow into S3 object keys through the stream ID. This doc records why that's safe.

## How stream IDs are constructed

```erlang
%% rabbit_stream_queue.erl
stream_name(#resource{virtual_host = VHost, name = Name}) ->
    Timestamp = erlang:integer_to_binary(erlang:system_time()),
    osiris_util:to_base64uri(<<VHost/binary, "_", Name/binary, "_", Timestamp/binary>>).
```

The result is used as the stream ID and embedded in S3 keys:

```
rabbitmq/stream/<StreamId>/data/<offset>.<uid>.fragment
rabbitmq/stream/<StreamId>/metadata/root.<epoch>.<uid>.manifest
rabbitmq/stream/<StreamId>/metadata/<offset>.<uid>.group
```

S3 object keys have a maximum length of 1,024 bytes. The stream ID is the variable component. In practice, the stream ID is bounded by the filesystem: Osiris uses it as a directory name, and common filesystems (ext4, XFS) limit filenames to 255 bytes. The filesystem rejects overly long stream names before S3 would.

## Why this is safe

### `to_base64uri` is a strict character filter

Only `A-Za-z0-9_-=` survive. Everything else (including `/`, `.`, `..`, `%`, `\0`, spaces, unicode) is replaced with `_`. This is a one-way sanitizer, not a reversible encoding.

A queue name like `../../bucket-root/evil` becomes `______bucket-root_evil`. No path traversal, no structural injection.

### Timestamp prevents collisions

`erlang:system_time()` (nanosecond precision) is appended before encoding. Users cannot control this value. Even if two queue names produce identical prefixes after encoding, the timestamp differentiates them.

### Cross-tenant isolation

Two tenants in different vhosts cannot produce colliding stream IDs because:
1. The vhost name is part of the input
2. The timestamp differs between declarations
3. Even if the encoded prefix collides (due to lossy `_` replacement), the full ID won't match

### Double encoding in HTTP path

`key_to_path/1` applies `uri_string:quote(Key, "/")` before sending to S3. This percent-encodes the `=` characters from base64url padding in stream IDs. S3 [classifies `=` as requiring special handling](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html#object-key-guidelines) in key names. The percent-encoding is standard HTTP path behavior: S3 decodes it back to the literal character for storage.

The [safe character set for S3 keys](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html#object-key-guidelines) is `0-9 a-z A-Z ! - _ . * ' ( )`. Our keys use only `A-Za-z0-9_-=./`, all of which are either safe or handled correctly by percent-encoding in the HTTP request path.

### The `_` separator ambiguity

The format `VHost_Name_Timestamp` uses `_` as separator, but `_` is also in the allowed charset and is the replacement character. This means:

```
vhost="a", name="b_c"  →  "a_b_c_<ts>"
vhost="a_b", name="c"  →  "a_b_c_<ts>"
```

This is not a security issue because:
- The stream ID is never parsed back into components
- It's used as an opaque identifier
- The timestamp ensures uniqueness regardless

## What a malicious user cannot do

- Overwrite another stream's objects (different stream ID → different key prefix)
- Traverse out of the `rabbitmq/stream/` prefix (no `/` survives encoding)
- Inject S3 key structure like `/metadata/` or `/data/` into the stream ID
- Cause key collisions with other tenants (timestamp uniqueness)

## What a malicious user can do

- Create many streams → many objects in the bucket (resource exhaustion, bounded by RabbitMQ queue limits)

These are operational concerns handled by quotas, not security vulnerabilities.
