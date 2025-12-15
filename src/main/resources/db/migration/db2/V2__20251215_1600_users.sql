-- ============================================
-- Table: users
-- No dependencies
-- Generated: 2025-12-15 16:00:51
-- Statements: 4
-- ============================================

create table "public"."users" (
    "id" integer not null default nextval('users_id_seq'::regclass),
    "name" character varying(255),
    "created_at" timestamp without time zone default CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX users_pkey ON public.users USING btree (id);

alter table "public"."users" add constraint "users_pkey" PRIMARY KEY using index "users_pkey";

alter sequence "public"."users_id_seq" owned by "public"."users"."id";

-- End of users migration
