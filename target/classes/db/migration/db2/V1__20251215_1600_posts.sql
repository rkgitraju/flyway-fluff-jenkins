-- ============================================
-- Table: posts
-- No dependencies
-- Generated: 2025-12-15 16:00:51
-- Statements: 4
-- ============================================

create table "public"."posts" (
    "id" integer not null default nextval('posts_id_seq'::regclass),
    "title" character varying(255),
    "content" text,
    "created_at" timestamp without time zone default CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX posts_pkey ON public.posts USING btree (id);

alter table "public"."posts" add constraint "posts_pkey" PRIMARY KEY using index "posts_pkey";

alter sequence "public"."posts_id_seq" owned by "public"."posts"."id";

-- End of posts migration
