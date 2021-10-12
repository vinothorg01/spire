-- A modified version of the schema export from Paul Fryzel
-- https://gist.githubusercontent.com/paulfryzel/4d3d9818adb0760cc207186fefbba720/raw/9926bebf535fe6668842894901362c1ba861d935/gistfile1.txt
-- Removed permission statements (grants and ownership)
-- Remove views
create table assembly_history (
	id serial not null constraint assembly_history_pkey primary key,
	trait_id integer not null,
	mlflow_run_id varchar(75),
	run_time timestamp,
	recipe jsonb,
	stats jsonb,
	info jsonb,
	warnings jsonb,
	status varchar(50),
	processing_time integer,
	notebook_url varchar(200),
	error text,
	traceback varchar
);

create table datasets (
	id uuid not null constraint datasets_pk primary key,
	definition jsonb not null
);

create index idx_datasets_id on datasets (id);

create table demo (
	run_id varchar(100) not null constraint demo_run_id_key unique,
	target varchar(100) not null,
	brand varchar(100) not null,
	training_response jsonb not null,
	last_trained varchar(100)
);

create table dfp_orders (
	id serial not null constraint dfp_orders_id_key unique,
	advertiser varchar(100) not null,
	order_id bigint not null constraint dfp_orders_order_id_key unique,
	start_date date not null,
	last_processed_date date,
	active boolean not null
);

create table history (
	id uuid not null,
	workflow_id uuid not null,
	stage varchar(50) not null,
	arg_date timestamp,
	execution_date timestamp,
	stats jsonb,
	info jsonb,
	status varchar(50),
	warnings jsonb,
	error text,
	traceback varchar
);

create index idx_history_arg_date_status_stage on history (arg_date, status, stage);

create index idx_history_id on history (id);

create index idx_history_workflow_id on history (workflow_id);

create table model_scoring_status (
	trait_id serial not null constraint model_scoring_status_trait_id_key unique,
	run_id varchar(100) not null constraint model_scoring_status_run_id_key unique,
	scoring_response jsonb not null,
	last_scored date,
	ready_to_process boolean default false not null
);

create table model_training_status (
	trait_id serial not null constraint model_training_status_trait_id_key unique,
	run_id varchar(100) not null constraint model_training_status_run_id_key unique,
	training_response jsonb not null,
	last_trained date,
	ready_to_score boolean default false not null
);

create table models (id uuid not null, definition jsonb not null);

create table outputs (id uuid not null, definition jsonb not null);

create table postprocess (
	workflow_id uuid,
	id uuid not null constraint postprocess_workflows_pkey primary key,
	type varchar(100)
);

create table workflows (
	id uuid not null constraint id_unique unique,
	name varchar(100),
	description text,
	created_at date default now(),
	modified_at date default now(),
	enabled boolean default false,
	is_proxy boolean default false
);

create table cluster_status (
	workflow_id uuid constraint cluster_status_workflow_id_fkey references workflows (id),
	stage varchar(100),
	id uuid not null constraint cluster_status_pkey primary key
);

create table assembly_status (
	cluster_id varchar(100),
	run_id integer,
	id uuid not null constraint assembly_status_pkey primary key constraint assembly_status_id_fkey references cluster_status
);

create index idx_workflows_id on workflows (id);

create table recipes (
	trait_id serial not null constraint recipes_trait_id_key unique,
	trait_name varchar(100) not null,
	recipe jsonb not null,
	vendor varchar(50) not null
);

create table schedules (workflow_id uuid not null, definition jsonb);

create index idx_schedules_workflow_id on schedules (workflow_id);

create table scoring_history (
	id serial not null constraint scoring_history_pkey primary key,
	trait_id integer not null,
	mlflow_run_id varchar(75),
	run_time timestamp,
	recipe jsonb,
	stats jsonb,
	info jsonb,
	warnings jsonb,
	status varchar(50),
	processing_time integer,
	notebook_url varchar(200),
	error text,
	traceback varchar
);

create table scoring_status (
	cluster_id varchar(100),
	run_id integer,
	id uuid not null constraint scoring_status_pkey primary key constraint scoring_status_id_fkey references cluster_status
);

create table segments (
	id uuid not null constraint segments_pkey primary key,
	name varchar not null,
	categories character varying
);

create table spire_recipes (
	trait_id integer not null constraint spire_recipes_trait_id_key unique,
	spire_id uuid not null,
	trait_name varchar(200) not null,
	recipe jsonb not null,
	vendor varchar(50) not null
);

create table spire_scheduler (
	id serial not null constraint spire_scheduler_id_key unique,
	trait_id integer not null constraint spire_scheduler_trait_id_key unique,
	trait_name varchar(100) not null constraint spire_scheduler_trait_name_key unique,
	run_id varchar(100) constraint spire_scheduler_run_id_key unique,
	response jsonb not null,
	last_run timestamp,
	ready boolean default false not null,
	project_type varchar(50) not null,
	process_name varchar(50) not null
);

create table tags (
	id uuid not null constraint tags_pkey primary key,
	label varchar(100) not null constraint tags_label_key unique
);

create table tasks (
	id uuid not null,
	name varchar(100),
	description text,
	created_at date default now(),
	modified_at date default now(),
	enabled boolean default false
);

create table thresholds (
	id uuid not null constraint thresholds_pkey primary key,
	strategy varchar(100),
	thresholds double precision,
	buckets text
);

create table training_history (
	id serial not null constraint training_history_pkey primary key,
	trait_id integer not null,
	mlflow_run_id varchar(75),
	run_time timestamp,
	recipe jsonb,
	stats jsonb,
	info jsonb,
	warnings jsonb,
	status varchar(50),
	processing_time integer,
	notebook_url varchar(200),
	error text,
	traceback varchar
);

create table training_schedule (
	id serial not null constraint training_schedule_id_key unique,
	trait_id integer not null,
	run_interval varchar(50) not null,
	start_date timestamp default now()
);

create table training_set_assembly_status (
	trait_id serial not null constraint training_set_assembly_status_trait_id_key unique,
	assembly_response jsonb not null,
	train_set_last_assembled date,
	ready_to_train boolean default false not null
);

create table training_status (
	cluster_id varchar(100),
	run_id integer,
	id uuid not null constraint training_status_pkey primary key constraint training_status_id_fkey references cluster_status
);

create table trait_recipes (
	trait_id serial not null constraint trait_recipes_trait_id_key unique,
	trait_name varchar(200) not null,
	recipe jsonb not null,
	vendor varchar(50) not null,
	spire_id uuid
);

create table trait_workflows (
	trait_id integer not null constraint trait_workflows_trait_id_key unique,
	workflow_id uuid not null constraint trait_workflows_workflow_id_key unique
);

create table workflow_datasets (
	workflow_id uuid not null,
	dataset_id uuid not null
);

create index idx_workflows_datasets_workflow_id on workflow_datasets (workflow_id);

create table workflow_models (
	workflow_id uuid not null,
	model_id uuid not null
);

create table workflow_outputs (
	workflow_id uuid not null,
	output_id uuid not null
);

create table workflow_tags (
	workflow_id uuid constraint workflow_tags_workflow_id_fkey references workflows (id),
	tag_id uuid constraint workflow_tags_tag_id_fkey references tags
);

create table features (
	id uuid not null constraint features_pkey primary key,
	definition json
);

create table workflow_features (
	workflow_id uuid constraint workflow_features_workflow_id_fkey references workflows (id),
	features_id uuid constraint workflow_features_features_id_fkey references features
);
