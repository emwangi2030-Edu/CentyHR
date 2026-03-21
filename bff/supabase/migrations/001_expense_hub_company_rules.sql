-- Expense Hub: policy / workflow / feature flags per ERP company (Pay Hub + BFF).
-- Apply in Supabase SQL editor or via supabase db push / psql.
-- BFF uses SUPABASE_SERVICE_ROLE_KEY only (bypasses RLS).

create table if not exists public.expense_hub_company_rules (
  company_key text primary key,
  policy jsonb not null default '{}'::jsonb,
  workflow jsonb not null default '{}'::jsonb,
  feature_flags jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now()
);

comment on table public.expense_hub_company_rules is
  'Expense policy and workflow metadata keyed by ERPNext Company name; canonical claims remain in ERP.';

alter table public.expense_hub_company_rules enable row level security;

-- No policies for anon/authenticated: clients cannot read/write; service_role bypasses RLS.

create index if not exists expense_hub_company_rules_updated_at_idx
  on public.expense_hub_company_rules (updated_at desc);
