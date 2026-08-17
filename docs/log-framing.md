This doc is a very quick look into the framing and overhead of a message in a stream.

Publish one message with 100 bytes of payload.

```
stream-perf-test --producers 1 --consumers 0 --streams my-stream --pmessages 1 --size 100
```

Segment framing: each segment starts with a magic (4 bytes) and version (4 bytes).

```
% hexdump 00000000000000000000.segment
0000000 534f 4c49 0000 0100 0050 0100 0000 0100
0000010 0000 9d01 2b8d 5e6f 0000 0000 0000 0100
0000020 0000 0000 0000 0000 28f5 c20c 0000 6d00
0000030 0000 0000 0000 0000 0000 6900 5300 a075
0000040 0064 0100 8d9d 6f2b 0026 0000 0000 0000
0000050 0000 0000 0000 0000 0000 0000 0000 0000
*
00000a0 0000 0000 0000
00000a5
% hexdump 00000000000000000000.index
0000000 534f 4949 0000 0100 0000 0000 0000 0000
0000010 0000 9d01 2b8d 5e6f 0000 0000 0000 0100
0000020 0000 0800 0000
0000025
```

Each chunk has a 48-byte header, which records in a single byte the size of the bloom filter that follows it. A chunk with no filter values carries no filter bytes at all, as in the dump below (`filter_size => 0`), so its entries begin 48 bytes in; a chunk that does carry a filter adds `filter_size` bytes, 16 by default. Each message is then stored in AMQP 1.0 framing.

```erlang
> Fd = osiris_log:dump_init("/path/to/00000000000000000000.segment").
> #{data := Data} = osiris_log:dump_chunk(Fd).
#{data =>
      <<0,0,0,105,0,83,117,160,100,0,0,1,157,141,43,111,38,0,0,
        0,0,0,0,0,0,0,0,0,...>>,
  position => 8,timestamp => 1776189927262,type => 0,
  data_size => 109,epoch => 1,filter_size => 0,
  num_records => 1,next_position => 165,num_entries => 1,
  chunk_id => 0,crc => 4113042626,trailer_size => 0,
  chunk_filter => <<>>,crc_match => true}
> osiris_log:dump_chunk(Fd).
eof
> byte_size(Data).
109
> <<0:1, Len0:31/unsigned, Rem0/binary>> = Data.
<<0,0,0,105,0,83,117,160,100,0,0,1,157,141,43,111,38,0,0,
  0,0,0,0,0,0,0,0,0,0,...>>
> <<Record0:Len0/binary, Rem1/binary>> = Rem0.
<<0,83,117,160,100,0,0,1,157,141,43,111,38,0,0,0,0,0,0,0,
  0,0,0,0,0,0,0,0,0,...>>
> Rem1.
<<>>
> Len0.
105
> amqp10_framing:decode_bin(Record0).
[{'v1_0.data',<<0,0,1,157,141,43,111,38,0,0,0,0,0,0,0,0,
                0,0,0,0,0,0,0,0,0,0,...>>}]
```

With simple records (i.e. no client-side batching), each record in a chunk costs 4 bytes of overhead to describe the record length.

Publishing at higher throughput increases chunk size, decreasing overhead in the segment file and for replication. Fewer, larger chunks also means fewer records in the index file. Each index record (one per chunk) costs 29 bytes. Index files are reconstructed during replication, not replicated directly, so this impacts on-disk storage and not network bandwidth.
