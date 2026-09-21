# Object store portability

The plugin is named for S3, but the remote tier sits behind an Erlang behaviour
and there are now two production backends behind it. This document records how
far that abstraction actually goes, what was needed to reach Google Cloud
Storage and Azure Blob, and what each one still costs.

## The behaviour

[`rabbitmq_stream_s3_api`](../src/rabbitmq_stream_s3_api.erl) is both the
behaviour and the dispatcher every caller in the plugin goes through. The
backend module is resolved per call from
`rabbitmq_stream_s3_config:api_backend/0` (application environment
`rabbitmq_stream_s3.rabbitmq_stream_s3_api`, defaulting to
`rabbitmq_stream_s3_api_aws`).

Fifteen required callbacks:

| Group | Callbacks |
|---|---|
| Synchronous reads | `get/2`, `get_range/3` |
| Asynchronous reads | `get_range_async/3`, `match_async/3`, `handle_async/3`, `cancel_async/2` |
| Writes | `put/3` |
| Streaming writes | `stream_put/3`, `stream_data/2`, `stream_finish/2`, `stream_abort/1` |
| Housekeeping | `delete/2`, `list/3`, `check_bucket/1` |
| Lifecycle | `start_link/0` |

One more is optional: `endpoint/0`, the host the connection pools connect to. A
backend that implements it needs pools. A backend that reaches no network omits
it and gets none.

Observability splits in two. The dispatcher counts operations and bytes and
keeps the read and write duration histograms. `rabbitmq_stream_s3_http` counts
requests, refusals, timeouts and in-flight connections. A backend implements
protocol and encoding only.

Three implementations exist, which is the useful evidence that the interface is real
rather than aspirational: `rabbitmq_stream_s3_api_aws`,
`rabbitmq_stream_s3_api_azure`, and `rabbitmq_stream_s3_api_fs`, a filesystem
backend used by the test suites.

What the two network backends share is `rabbitmq_stream_s3_http`. Sending a
request on a pooled `gun` connection, buffering an async response, recovering a
Range answered with 200, draining an error body and timing a request out is
transport, not protocol. A second store speaking a different API over the same
pipe needed all of it unchanged.

The request counters live there for the same reason. A backend installs them
from its `start_link/0`, because only the backend knows whether it uses HTTP.

### Where the abstraction leaks

- `request_opts()` carries `crc32` and `unsigned_payload`, both of which are
  shaped by S3's checksum and payload-signing rules.
- `check_bucket/1`'s error contract names `no_such_bucket` and `access_denied`.
- The supervision tree, not the behaviour, is where AWS is named.
  `rabbitmq_stream_s3_sup` resolves the API backend's own child from config but
  still lists its two `rabbitmq_stream_s3_api_aws_pool` children by module. The
  module name is now the whole of it. The pool asks `rabbitmq_stream_s3_api` for
  the host to connect to, and `rabbitmq_stream_s3_api:needs_http_pool/0` decides
  whether it starts at all, so the filesystem backend and the fault-injecting
  wrapper around it both return `ignore`.
- The port a pool connects on and whether it uses TLS are the HTTP client's
  configuration (`stream_s3.http.port`, `stream_s3.http.tls`) rather than the
  backend's, since they describe how a host is reached and not what its API
  is.

The async callbacks, by contrast, are generic. `Msg` is `term()`, so each
backend matches its own message shapes. The two network backends delegate to
`rabbitmq_stream_s3_http`, which matches gun messages. `api_fs` matches
`{'$async', Req, Msg}`.

## Authentication is hand-rolled

There is no AWS SDK in the dependency list (`rabbit`, `rabbit_common`,
`osiris`, `khepri`, `gun`, `rabbitmq_prometheus`). The plugin uses neither
`erlcloud` nor `aws-erlang`. `rabbitmq_stream_s3_auth_aws` implements
AWS4-HMAC-SHA256 itself on `crypto`. `rabbitmq_stream_s3_api_aws` uses `xmerl`
to parse `ListObjectsV2` responses and to build multi-object `Delete` bodies.

`rabbitmq_stream_s3_auth_aws` also holds the credential chain: static keys from
`rabbitmq.conf` behind `allow_static_credentials`, then container credentials
via `AWS_CONTAINER_CREDENTIALS_FULL_URI`, then IMDSv2. That is roughly 300 lines
whose equivalent each new cloud needs.

This is a deliberate house style, and it is the right baseline for judging the
ports below: the question is never "is there an SDK?" but "how much protocol is
there to write?".

### The auth interface

`rabbitmq_stream_s3_auth` is a second behaviour and dispatcher, resolved from
`rabbitmq_stream_s3_config:auth_backend/0`, so that how a request proves its
identity varies independently of which wire protocol carries it. The XML API is
one protocol several stores accept. The credentials differ per store.

Its callback is `authorize(request(), req_headers())`, returning the headers to
send, rather than something like `get_credentials/0`. Schemes disagree on what
authorization needs: SigV4 derives its header from the method, path and payload
digest, while an OAuth bearer token ignores all of them and has no key pair to
hand back. A credential-shaped callback would push that difference out to every
call site.

Four backends implement it:

| Module | Scheme | For |
|---|---|---|
| `rabbitmq_stream_s3_auth_aws` | AWS SigV4 | S3, and anything S3-compatible taking HMAC keys |
| `rabbitmq_stream_s3_auth_bearer` | OAuth2 bearer token | Google Cloud Storage, Azure Blob |
| `rabbitmq_stream_s3_auth_azure` | Azure Shared Key | Azure Blob without a managed identity, and Azurite |
| - | none | backends with nothing to authorize, which start no auth backend at all |

### Who starts what

An API backend starts the auth backend it needs, through `start_link/0` on the
API behaviour. `rabbitmq_stream_s3_api_aws:start_link/0` installs the
transport's counters and then calls `rabbitmq_stream_s3_auth:start_link/0`. The
module that resolves to stays a detail of the auth interface.
`rabbitmq_stream_s3_api_fs:start_link/0` returns `ignore`: a backend that reads
and writes files authorizes nothing and installs no counters.

The supervisor starts the configured API backend rather than naming
`rabbitmq_stream_s3_api_aws` outright, so what ends up supervised is whatever
that backend chose to start. Each backend answers for itself, which removes the
question a backend gating itself in `init/1` used to answer on another's
behalf.

The two connection pools return `ignore` for a backend that implements no
`endpoint/0`, which covers the filesystem backend and the fault-injecting
wrapper around it. Every other backend gets them, so one that shares this HTTP
client needs no change there.

`rabbitmq_stream_s3_auth_aws` holds the credential chain, the region lookup and
the SigV4 signing. Addressing stays with the client, in two named pieces:
`endpoint/0` is the host without the bucket, which the pool connects to and TLS
verifies, and `request_host/0` puts the bucket under it. The client sets `host`
before handing a request to the auth backend, which signs the host it is given
rather than deriving one, so the dependency runs one way and a scheme that does
not sign keeps virtual-hosted addressing.

`endpoint/0` consults the auth backend for a region only when no endpoint is
configured, since a configured endpoint is the host outright.

### Bearer tokens

`rabbitmq_stream_s3_auth_bearer` covers GCP and Azure in one module: both fetch
an access token from a link-local metadata server and then send it as
`Authorization: Bearer`, differing only in the request shape and in whether
`expires_in` comes back as a number (GCP) or a decimal string (Azure). S3 has
no bearer mode, which is why SigV4 stays a separate backend rather than a third
provider here. `stream_s3.auth = bearer` selects it and
`stream_s3.bearer.provider` picks the cloud.

Tokens are cached in an ETS table the process owns and refreshed at the halfway
point of their life, so a transient metadata failure has room to retry before
anything expires.

Because the client sets `host` itself, a backend that adds only an
`authorization` header still addresses the right bucket. That is what makes a
credential-less HTTP store, such as a local mock, reachable at all.

## Google Cloud Storage

### What is implemented

GCS exposes an XML API that accepts AWS Signature Version 4 signed with
interoperability HMAC keys, with `s3` as the service name in the credential
scope. The signing code therefore needs no changes at all. The gap was
addressing: `hostname/1` derived the host as `s3.<region>.<tld>`, and GCS has a
single global XML API host with no region in it.

`stream_s3.endpoint` sets the endpoint host directly, replacing the derived
host. Addressing stays virtual-hosted, so requests go to `<bucket>.<endpoint>`:

```ini
stream_s3.bucket = my-rabbitmq-streams-bucket
stream_s3.endpoint = storage.googleapis.com
stream_s3.streaming_upload = multipart
stream_s3.allow_static_credentials = true
stream_s3.access_key_id = GOOG1E...
stream_s3.secret_key = ...
```

No region is configured because an endpoint host carries none to match. Nothing
is looked up from instance metadata, which would describe the instance rather
than the store, and the credential scope falls back to `auto`.

Coverage is unit tests over the addressing and signing path: that `endpoint/0`
returns the configured endpoint verbatim, that `request_host/0` puts the bucket
under it, and that the credential scope comes out as
`<date>/auto/s3/aws4_request`. None of it has been run against real GCS.

The same setting reaches any S3-compatible store. The alternative is to pick a
region name and map it to a TLD through `stream_s3.region_endpoints` so that
`s3.<region>.<tld>` resolves to the endpoint, which is what the Jepsen harness
does for MinIO.

### What is not implemented

Addressing was the first step, not the whole port. Everything below is from
Google's documentation, not from a run against real GCS:

- **Checksums.** GCS validates with CRC32C (Castagnoli) and MD5, through
  `x-goog-hash`. It implements neither the CRC32 (IEEE/zlib) that
  `erlang:crc32` computes nor AWS's `x-amz-checksum-crc32` header, so it
  presumably ignores that header. Nothing has confirmed that.

  Which paths send what matters more than the header itself:

  | Path | Integrity header |
  |---|---|
  | `put/3` | `x-amz-checksum-crc32`, but only when the caller passes `crc32` in the request options. No caller in the plugin does, so these objects upload with no integrity header at all. |
  | `stream_put/3`, `chunked` | `x-amz-checksum-crc32` in the aws-chunked trailer |
  | `stream_put/3`, `multipart` | `content-md5` per part, and no CRC32 |
  | multi-object `delete/2` | `x-amz-checksum-crc32` over the `Delete` document |

  A GCS deployment sets `streaming_upload = multipart`, so a fragment already
  travels under `content-md5`, which GCS does verify. That preserves what
  [upload-crc-strategy.md](./investigations/upload-crc-strategy.md) wanted from
  the upload checksum: the store rejects a PUT whose bytes changed on disk or in
  transit. On that path CRC32 is not in play at all, and the only header GCS
  would ignore is the one on the `Delete` document.

  No setting disables the CRC32 header. `stream_s3.verify_crc_on_read` is the
  read path and is unrelated.

  A crc32c implementation is therefore not the gap, and OTP has none. The
  cheaper change in the same spirit is to send `content-md5` from
  `api_aws:put/3`, as `api_azure:put/3` already does. Every S3-compatible store
  verifies it, it needs no new checksum code, and it closes the one real hole
  above: manifest and index objects currently upload unprotected.

- **Server-side encryption headers.** `sse_headers/1` always sends
  `x-amz-server-side-encryption: AES256`, or SSE-KMS plus an encryption context
  when `kms_key_id` is set. GCS encrypts at rest unconditionally and takes none
  of these headers. Whether it ignores them or rejects the request decides
  whether the write path works at all, and nobody has tested which. SigV4 signs
  the header, so a GCS deployment cannot simply drop it without changing what
  the signature covers.

- **`x-amz-expected-bucket-owner`.** Sent when `stream_s3.account_id` is
  configured. GCS has no equivalent, so leave `account_id` unset.

- **Path-style addressing.** This is the plugin's limitation, not a GCS one.
  GCS serves `https://storage.googleapis.com/<bucket>/<object>`, but
  `rabbitmq_stream_s3_api_aws:request_host/0` always builds
  `<bucket>.<endpoint>`, so the bucket has to resolve as a subdomain. That also
  rules out any store that offers path-style only.
  `rabbitmq_stream_s3_api_azure` has the path-style form already, through
  `stream_s3.azure.path_style`. Nothing equivalent exists on the S3 client.

Multi-object delete, by contrast, needs no work: GCS's XML API takes `POST
/?delete` with the same `Delete` document `delete_many_body/1` already builds,
up to 1,000 keys per request. It is explicitly non-atomic and can partially
fail, which is what the behaviour's `delete/2` documentation already permits.

### Uploads

`stream_put/3` has two implementations, chosen by
`rabbitmq_stream_s3_config:streaming_upload/0`.

`chunked` is S3's aws-chunked encoding with a trailing checksum: one PUT for a
whole fragment, never buffering more than a chunk of it, with a CRC32 computed
while streaming and sent after the body. It is an S3 extension, and when AWS
turned its default integrity protections on it broke against every
S3-compatible store that had not implemented it, GCS included.

`multipart` is the portable alternative: `InitiateMultipartUpload`, a `PUT` per
part, then `CompleteMultipartUpload`. Each part carries `content-md5`, so bytes
are still covered end to end without the trailer. The cost is why it is not
simply used everywhere - one fragment becomes several requests instead of one,
and an uploader holds a whole part rather than a chunk - so `chunked` stays the
default and `stream_s3.streaming_upload` selects it for a store that needs it.

A completion can fail with status 200 and an `<Error>` body, because the store
answers before it has finished assembling the object, so the body is parsed
rather than the status trusted. A failed or abandoned upload is aborted, since
its parts stay billable until it is.

### Credentials, and why they do not need a native backend

The arrangement above needs interoperability HMAC keys. "Interoperability" is
Google's product name for the S3-compatibility mode itself, and an HMAC key is
an access ID and secret pair: the direct analogue of an AWS access key ID and
secret, carrying the same drawbacks the plugin already warns about under
`allow_static_credentials`.

The XML API also accepts `Authorization: Bearer` OAuth2 tokens. The GCE metadata
server hands one out at
`metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token`
with `Metadata-Flavor: Google`, which reuses the existing metadata-refresh
`gen_server` shape and replaces `sign_headers/8` with a single header. Less code
than SigV4, and it reaches Workload Identity.

The point for portability is that this is a change to one header. The xmerl
parsing, the ranged GETs and the `Delete` document all stay, so native
credentials are not an argument for a native backend.

This may not stay optional. The organization policy constraint
`constraints/storage.restrictAuthTypes` disables HMAC-signed requests, and when
it is set existing keys stop working and cannot be reactivated. In an
organization with that constraint, endpoint plus HMAC keys cannot work at all
and the Bearer path is the only way in.

### Why not a native GCS backend

Given the above, a second full backend against Google's JSON API buys very
little that the compatibility layer cannot reach. Resumable uploads offer true
crash-resume where multipart only restarts a part, and generation preconditions
are irrelevant here because manifest concurrency lives in Khepri. Against that,
a native backend means a second implementation of all fourteen callbacks, and
gives up the property that one backend covers AWS, MinIO, R2, Wasabi and GCS at
once.

## Azure Blob Storage

Implemented, as a genuine port rather than an addressing change:
`rabbitmq_stream_s3_api_azure`, selected with `stream_s3.api = azure`.

Everything below the headers is `rabbitmq_stream_s3_http`. What the port had to
write is the protocol:

- Every request declares an `x-ms-version`. Omitting it is a 400.
- A container is a path segment under the account's host rather than a
  subdomain, so the account is what a connection is opened to and the container
  is part of every path. `endpoint/0` and `request_host/0` are the same thing
  here.
- Success is `201 Created` for a write and `202 Accepted` for a delete, and
  deleting a blob that is already gone is a 404 where S3 answers 204 regardless.
  Callers delete to reach a state rather than to learn what was there, so 404 is
  reported as `ok`.
- `List Blobs` pages with `marker`/`NextMarker` over `EnumerationResults`
  instead of a continuation token over `ListBucketResult`.
- There is no multi-object delete, so `delete/2` over a list degrades to one
  request per key. The behaviour's documentation already allows non-atomic
  multi-delete, so this is legal, but it costs the reaper and GC a request per
  object rather than one per thousand. The Blob Batch API (`POST /?comp=batch`,
  multipart/mixed, at most 256 sub-requests each signed separately) would
  restore the batching and has not been written.

Statuses map back to the same error atoms the S3 client produces, because
`rabbitmq_stream_s3_replica_reader_core:is_retriable/1` matches those atoms and a
backend inventing its own vocabulary would have its transient failures
classified as fatal.

### Uploads, and the checksum that does not fit

`stream_put/3` uploads a fragment as blocks: `Put Block` per block, then one
`Put Block List` that commits them.

Azure would take a single streaming PUT of a known content length, which for a
64 MiB fragment is one request rather than nine. Blocks are used anyway because
of the checksum. S3's `chunked` mode sends a CRC32 *after* the body, which is
exactly the shape `stream_finish(State, Crc32)` was built for. Azure accepts an
integrity check only as a request header (`content-md5`, `x-ms-content-crc64`),
which means before the body. A single streaming PUT therefore could be covered
by nothing but TLS. A block can be: it carries a `content-md5` over exactly the
bytes being sent. The fragment's CRC32 still arrives too late to be a header,
and is recorded on the commit as `x-ms-meta-crc32`, so what the writer computed
stays with the object even though the service does not verify it.

Uncommitted blocks need no equivalent of `AbortMultipartUpload`: they are
invisible to readers and Azure discards them a week after the last write.

### Ranges

Azure's `Range` grammar is `bytes=start-end` and `bytes=start-` only. There is no
suffix form, so the `bytes=-N` that `range_spec()` allows - and that both the S3
and filesystem backends serve - has no equivalent, and `get_range/3` and
`get_range_async/3` answer it with `{error, not_supported}`.

This is the one place a backend does not honour the full behaviour, so it is
worth saying why it is refused rather than emulated. Emulating it means
resolving the blob's length with a Get Blob Properties request and subtracting,
which spends a round trip per read to hide the fact that the store does not do
this. Nothing would be paying that cost: no caller in the repository's history
has ever passed a negative range. The plugin does read the tail of an object -
a fragment's index - but the manifest records where that index starts, so the
read is expressed as `{IdxStartPos, undefined}`, an absolute open-ended range.
Every read the plugin makes knows its own offsets, which is why the suffix form
has been unused since the first commit.

`range_spec()` keeps the suffix form, because S3 and the filesystem backend do
serve it and narrowing the type to the least capable backend would lose that. A
caller that wants to read from the end is told the store cannot rather than
quietly charged for a second request.

### Query strings

Query parameters are percent-encoded with
`rabbitmq_stream_s3_http:compose_query/1` rather than
`uri_string:compose_query/1`, which writes a space as `+`. A bare `+` in a query
is ambiguous - Azure reads it as a space - so a signature computed by decoding it
as a literal plus is rejected with a 403 on any key containing a space. Encoding
a space as `%20` and a plus as `%2B` leaves nothing to interpret.

The S3 client still composes its `prefix` parameter the other way. SigV4 signs
the query as sent, so there is no 403 there. The exposure is that S3 reads a `+`
literally, so listing a prefix that contains a space matches nothing. That is
pre-existing and has not been changed here.

### Authentication

Two schemes, both already in the auth interface:

- `stream_s3.auth = bearer` with `stream_s3.bearer.provider = azure` fetches an
  Entra ID token from Azure IMDS. This is the right choice on an Azure VM, and
  the only one where an account disallows Shared Key.
- `stream_s3.auth = azure` (`rabbitmq_stream_s3_auth_azure`) signs with the
  storage account key. Structurally SigV4's smaller relative: an HMAC-SHA256
  over a canonical description of the request, differing in that the signed
  description is a fixed list of headers by position rather than a negotiated
  signed-headers set, and that the account name is part of the canonicalized
  resource rather than of a credential scope. It holds nothing refreshable, so
  its `start_link/0` returns `ignore`.

### Testing

`rabbitmq_stream_s3_api_azure_SUITE` runs against Azurite, Microsoft's emulator,
or against a real storage account when `AZURE_STORAGE_ACCOUNT`,
`AZURE_STORAGE_KEY` and `AZURE_STORAGE_CONTAINER` are set. It skips itself when
neither is reachable.

Azurite is what makes this testable without a subscription. Unlike the GCS
emulators measured below, it implements the whole surface the backend uses -
ranged reads, block uploads, `List Blobs` paging, container properties - and it
validates Shared Key signatures, so a signing mistake fails the way it would in
production rather than being ignored. It accepts OAuth tokens only over HTTPS
with `--oauth basic`, which is the practical reason the Shared Key backend
exists rather than only the bearer one.

What Azurite does not cover is the addressing a deployment uses: it puts the
account in the URL path, where a real account is a subdomain, and it serves plain
HTTP. Those paths are covered by unit tests and by pointing the suite at a real
account.

## Erlang libraries

For AWS, `erlcloud` and `aws-beam/aws-erlang` both exist and are maintained.
This plugin deliberately uses neither.

For GCP there is no well-maintained pure-Erlang library worth depending on. The
real options are Elixir (`goth` for auth, `google_api_storage` for the JSON
API). RabbitMQ already builds Elixir for the CLI, but adding it to a broker
plugin's runtime dependencies is a significant change, and metadata-server auth
is small enough to write directly.

For Azure, `erlazure` is the pure-Erlang option covering blob, queue and table
with Shared Key auth. It has been low-activity for years, and the port did not
need it: Shared Key signing is around 150 lines and the REST surface the plugin
uses is small.

## Local emulators

Measured against v1.56.1 of fake-gcs-server and v0.45.0 of storage-testbench, by
issuing the plugin's own request shapes over HTTP. Azurite is not in this table
because it is not partial: it serves every request the Azure backend makes, and
is what that backend's suite runs against. See its section above. Google ships no GCS emulator
in `gcloud`, unlike Pub/Sub, Datastore, Bigtable, Firestore and Spanner.

| Operation | fake-gcs-server | storage-testbench |
|---|---|---|
| Virtual-hosted addressing | yes | no, path-style only |
| `GET` object | yes | yes |
| `GET` range | yes, `206` | no, `416` |
| `ListObjectsV2` (`?list-type=2`) | yes, correct `ListBucketResult` | no, `404` |
| `PUT` object | no, `400 invalid uploadType` | yes |
| `DELETE` object | no, `405` | no, `405` |
| Multi-object delete (`?delete`) | no, `404` | no, `404` |
| `HEAD` bucket | no, `404` | not probed |
| SigV4 validation | none, credentials ignored | none |

Neither covers the whole backend. fake-gcs-server is the better of the two
because it routes virtual-hosted requests and serves ranged reads and
`ListObjectsV2` in the shape `list/3` parses, so it can exercise the read path
end to end. Objects have to be seeded through its JSON API. Its lack of SigV4
validation is convenient rather than limiting, since it accepts the plugin's
signed requests and ignores the signature.

MinIO remains the most useful local target overall. It implements the whole S3
surface the plugin uses, including chunked uploads and multi-object delete, and
the Jepsen harness already drives it. It verifies the endpoint override and the
full request set, but not GCS's semantics.

The practical arrangement is therefore MinIO for coverage and fake-gcs-server
for the GCS-specific read path, with real GCS required to close out the
streaming-upload question.

## A native GCS backend (JSON API) against the XML API

### What a native backend would add

- **Resumable uploads.** GCS's resumable protocol (initiate, session URI, then
  PUT per chunk) fits the `stream_put`/`stream_data`/`stream_finish` shape
  better than aws-chunked ever did, and it gives real crash-resume. Multipart
  already removed the urgency here: it holds one 8 MiB part rather than a whole
  64 MiB fragment, so the write path no longer depends on aws-chunked.
- **Batched delete**, through the JSON batch endpoint, which is what the reaper
  and GC want. Check the current guidance first. Google has moved users away
  from batch endpoints, and the present status is unclear.
- **Generation numbers and `ifGenerationMatch` preconditions.** More ergonomic
  than S3's, and irrelevant here: manifest concurrency lives in Khepri.

### What it would cost

A second full backend: 15 callbacks, JSON instead of xmerl, `pageToken`
pagination and a different list shape. The Azure port has since shown what that
costs in practice, and how much of it `rabbitmq_stream_s3_http` absorbs.

It also gives up the property that makes the S3 path attractive. One backend
covers AWS, MinIO, Scaleway, R2, Wasabi and GCS. A native GCS backend covers one
store, and fewer users would exercise it.

One detail cuts against native. GCS checksums are CRC32C (Castagnoli). The
plugin computes CRC32 through `erlang:crc32`, which is zlib/IEEE, a different
polynomial. Native checksum integration needs a crc32c implementation, and OTP
has none. That is real work rather than a free win.

### Where this leaves the decision

The bearer-token backend and `stream_s3.streaming_upload = multipart` both
exist, so the XML API now reaches GCS without static HMAC keys and without
aws-chunked. That was the cheap half of the decision, and it is done.

A native backend is worth reaching for only if a run against real GCS finds
something the XML API cannot express. Nobody has made that run yet.

Organization policy is the other forcing function. Some organizations disable
interop HMAC keys outright through `constraints/storage.restrictAuthTypes`.
Where that applies, use the bearer backend.
