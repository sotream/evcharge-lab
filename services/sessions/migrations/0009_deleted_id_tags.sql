-- idTags of deleted accounts (SHA-256, hex). With ACCEPT_UNKNOWN_IDTAGS an unknown tag is accepted as ad-hoc charging; these are blocked.
CREATE TABLE deleted_id_tags (id_tag_sha256 text PRIMARY KEY, deleted_at timestamptz NOT NULL DEFAULT now());
