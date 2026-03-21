-- Self-onboarding invites (BFF service role only; no client writes).

create table if not exists public.employee_invites (
  id uuid primary key default gen_random_uuid(),
  token text not null unique,
  email text not null,
  company_key text not null,
  invited_by_email text,
  status text not null default 'pending',
  expires_at timestamptz not null,
  created_at timestamptz not null default now(),
  constraint employee_invites_status_chk check (status in ('pending', 'completed', 'expired', 'cancelled'))
);

create index if not exists employee_invites_email_company_idx on public.employee_invites (email, company_key);
create index if not exists employee_invites_expires_idx on public.employee_invites (expires_at);

alter table public.employee_invites enable row level security;

comment on table public.employee_invites is 'HR-issued onboarding links; completed when Employee is created via public BFF endpoint.';
