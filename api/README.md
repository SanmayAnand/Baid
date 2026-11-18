# api/

[`openapi.yaml`](openapi.yaml) is the HTTP API v1 contract (OpenAPI 3.1). The Python SDK
and memharness are built against it.

It is kept honest by `crates/server/tests/contract.rs`, which runs in `just check` and CI and
fails when the server and the spec drift apart:

- the spec's routes equal `server::http::ROUTES`, and its error codes equal `ApiError::CODES`;
- every documented request example is accepted, and the response matches the documented
  success schema (field names, types, required fields, no undocumented fields);
- closed request bodies reject unknown fields with a documented `Error` body;
- every authenticated route returns the documented 401 without a key;
- every `$ref` resolves, `operationId`s are unique, and path parameters are declared.

Change the server and this file in the same pull request. API v1 only changes additively;
a breaking change means `/v2`.

For a full OpenAPI lint, run a linter of your choice locally, for example
`npx @redocly/cli lint api/openapi.yaml`.
