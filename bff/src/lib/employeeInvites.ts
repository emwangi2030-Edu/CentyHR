import { randomBytes } from "node:crypto";
import { createClient, type SupabaseClient } from "@supabase/supabase-js";
import * as config from "../config.js";

export type EmployeeInviteRow = {
  id: string;
  token: string;
  email: string;
  company_key: string;
  invited_by_email: string | null;
  status: string;
  expires_at: string;
  created_at: string;
};

let supabase: SupabaseClient | null | undefined;

function getSupabase(): SupabaseClient | null {
  if (supabase !== undefined) return supabase;
  const url = config.SUPABASE_URL?.trim();
  const key = config.SUPABASE_SERVICE_ROLE_KEY?.trim();
  if (!url || !key) {
    supabase = null;
    return null;
  }
  supabase = createClient(url, key, {
    auth: { persistSession: false, autoRefreshToken: false },
  });
  return supabase;
}

export function invitesAvailable(): boolean {
  return getSupabase() !== null;
}

function generateToken(): string {
  return randomBytes(24).toString("hex");
}

export async function insertEmployeeInvite(params: {
  email: string;
  company_key: string;
  invited_by_email: string;
  expires_in_days?: number;
}): Promise<{ token: string; expires_at: string } | null> {
  const client = getSupabase();
  if (!client) return null;
  const days = Math.min(30, Math.max(1, params.expires_in_days ?? 14));
  const expires_at = new Date(Date.now() + days * 86400000).toISOString();
  const token = generateToken();
  const { error } = await client.from("employee_invites").insert({
    token,
    email: params.email.trim().toLowerCase(),
    company_key: params.company_key,
    invited_by_email: params.invited_by_email,
    status: "pending",
    expires_at,
  });
  if (error) {
    console.error("[employee-invites] insert failed:", error.message);
    return null;
  }
  return { token, expires_at };
}

export async function getInviteByToken(token: string): Promise<EmployeeInviteRow | null> {
  const client = getSupabase();
  if (!client) return null;
  const { data, error } = await client
    .from("employee_invites")
    .select("*")
    .eq("token", token.trim())
    .maybeSingle();
  if (error || !data) return null;
  return data as EmployeeInviteRow;
}

export async function markInviteCompleted(token: string): Promise<boolean> {
  const client = getSupabase();
  if (!client) return false;
  const { error } = await client
    .from("employee_invites")
    .update({ status: "completed" })
    .eq("token", token.trim())
    .eq("status", "pending");
  return !error;
}
